package model

import (
	_ "github.com/go-sql-driver/mysql"
	"github.com/garyburd/redigo/redis"
	"database/sql"
	"strconv"
	"strings"
	"time"
	"encoding/json"
	"encoding/hex"
	"crypto/md5"
	"fmt"
	"log"
)


// Types

type Food struct {
	Id int		`json:"id"`
	Price int	`json:"price"`
	Stock int	`json:"stock"`
}

type Item struct {
	FoodId int		`json:"food_id"`
	FoodCount int	`json:"count"`
}

type Order struct {
	Id string		`json:"id"`
	UserId int		`json:"user_id"`
	Items []Item	`json:"items"`
	Total int		`json:"total"`
}


// Databases

var db *sql.DB
var redisPool redis.Pool

var addFoodCountScript *redis.Script
var tryMakeOrderScript *redis.Script

func InitDatabases(mysqlDSN, redisDSN string) {
	db, _ = sql.Open("mysql", mysqlDSN)

	redisPool = redis.Pool{
		MaxIdle: 3,
		IdleTimeout: 240 * time.Second,
		Dial: func () (redis.Conn, error) {
			c, err := redis.Dial("tcp", redisDSN)
			if err != nil {
				return nil, err
			}
			return c, nil
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}

	addFoodCountScript = redis.NewScript(0, `
		local cartId = ARGV[1]
		local foodId = ARGV[2]
		local foodCount = tonumber(ARGV[3])
		local cart = "cart:" .. cartId .. ":foods"
		local items = redis.call("HVALS", cart)
		local sum = tonumber(items[1] or "0") + tonumber(items[2] or "0") + tonumber(items[3] or "0")
		if sum + foodCount > 3 then
			return 0
		else
			redis.call("HINCRBY", cart, foodId, foodCount)
			return 1
		end
	`)

	tryMakeOrderScript = redis.NewScript(0, `
		local userId = ARGV[1]
		local cartId = ARGV[2]
		if redis.call("EXISTS", "user:" .. userId .. ":order") == 1 then
			return -1
		else
			local items = redis.call("HGETALL", "cart:" .. cartId .. ":foods")
			for i = 1, 6 do
				items[i] = items[i] or "0"
			end
			local food1 = items[1]
			local food2 = items[3]
			local food3 = items[5]
			local dec1 = tonumber(items[2]) or 0
			local dec2 = tonumber(items[4]) or 0
			local dec3 = tonumber(items[6]) or 0
			local foods = redis.call("HMGET", "foodstock", food1, food2, food3)
			local amount1 = tonumber(foods[1]) or 0
			local amount2 = tonumber(foods[2]) or 0
			local amount3 = tonumber(foods[3]) or 0
			if (amount1 >= dec1 and amount2 >= dec2 and amount3 >= dec3) then
				redis.call("HMSET", "foodstock", food1, amount1 - dec1, food2, amount2 - dec2, food3, amount3 - dec3)
				redis.call("RENAME", "cart:" .. cartId .. ":foods", "user:" .. userId .. ":order")
				return 1
			else
				return -2
			end
		end
	`)

	for i := 0; i < 250; i++ {
		go RedisWorker()
	}
}


// Caches

type User struct {
	Id int
	Pwd string
}
// Fixed
var UserCache map[string]User
var TokenCache map[int]string
// Variable
var FoodsCache [101]Food
var FoodsString []byte


func IdEncode(uid int) string {
	bstr := []byte("AAAAA")
	for i := 4; i >= 0; i-- {
		bstr[i] += byte(uid % 10)
		uid /= 10
	}
	return string(bstr)
}


func SyncFoodCache(interval time.Duration) {
	go func() {
		conn := redisPool.Get()
		defer conn.Close()
		for {
			time.Sleep(time.Second * interval)
			results, _ := redis.Ints(conn.Do("HGETALL", "foodstock"))
			for i := 0; i < len(results); i+=2 {
				FoodsCache[results[i]].Stock = results[i+1]
			}
			foodstr, _ := json.Marshal(FoodsCache[1:])
			FoodsString = foodstr
		}
	}()
}

func InitAllCache() {
	// food
	foodRows, _ := db.Query("SELECT id, price, stock FROM food")
	defer foodRows.Close()

	conn := redisPool.Get()
	defer conn.Close()

	var id, price, stock int
	for foodRows.Next() {
		foodRows.Scan(&id, &price, &stock)
		FoodsCache[id].Id = id
		FoodsCache[id].Price = price
		FoodsCache[id].Stock = stock
		conn.Do("HSETNX", "foodstock", strconv.Itoa(id), stock)
	}
	foodstr, _ := json.Marshal(FoodsCache[1:])
	FoodsString = foodstr

	SyncFoodCache(5)

	// user
	userRows, _ := db.Query("SELECT name, password, id FROM user ORDER BY id")
	defer userRows.Close()

	UserCache = make(map[string]User)
	TokenCache = make(map[int]string)
	var uid int
	var usr, pwd string
	for userRows.Next() {
		userRows.Scan(&usr, &pwd, &uid)
		UserCache[usr] = User{uid, pwd}
		hasher := md5.New()
		hasher.Write([]byte(pwd))
		TokenCache[uid] = fmt.Sprintf("%s%s", IdEncode(uid), hex.EncodeToString(hasher.Sum(nil)))
	}
}


// Job Queue

type Job struct {
	jobType int
	userId int
	cartId string
	foodId int
	foodCount int
	result *chan int
}
var JobQueue = make(chan Job, 2000)

func RedisWorker() {
	conn := redisPool.Get()
	defer conn.Close()

	for job := range JobQueue {
		var err error
		var result int

		switch job.jobType {
		case 1:
			result, err = redis.Int(addFoodCountScript.Do(conn, job.cartId, job.foodId, job.foodCount))
		case 2:
			result, err = redis.Int(tryMakeOrderScript.Do(conn, job.userId, job.cartId))
		}

		if err != nil {
			log.Fatal(err)
		}

		*(job.result) <- result
	}
}



// Cart

var randomNum = 1

func CreateCart(userId int) string {
	randomNum += 1
	cartId := fmt.Sprintf("%d_%d", userId, randomNum)
	return cartId
}

func AddFoodCount(cartId string, foodId int, foodCount int) bool {
	message := make(chan int)

	job := Job {
		jobType: 1,
		cartId: cartId,
		foodId: foodId,
		foodCount: foodCount,
		result: &message,
	}

	JobQueue <- job

	return (<-message == 1)
}


// Order

func TryMakeOrder(userId int, cartId string) int {
	message := make(chan int)

	job := Job {
		jobType: 2,
		cartId: cartId,
		userId: userId,
		result: &message,
	}

	JobQueue <- job

	return <-message
}

func GetOrderItems(userId int) []Item {
	conn := redisPool.Get()
	defer conn.Close()

	key := fmt.Sprintf("user:%d:order", userId)
	result, _ := conn.Do("HGETALL", key)

	foods := result.([]interface{})
	count := len(foods) / 2
	if count == 0 {
		return nil
	}

	items := make([]Item, count)
	for i := 0; i < count; i++ {
		foodId, _ := strconv.Atoi(string(foods[2*i].([]byte)))
		foodCount, _ := strconv.Atoi(string(foods[2*i+1].([]byte)))
		items[i] = Item{foodId, foodCount}
	}

	return items
}

func GenerateOrder(userId int, items []Item) Order {
	total := 0
	for _, item := range items {
		price := FoodsCache[item.FoodId].Price
		total += price * item.FoodCount
	}
	return Order{
		Id: strconv.Itoa(userId),
		UserId: userId,
		Items: items,
		Total: total,
	}
}

func GetAllOrders() []Order {
	conn := redisPool.Get()
	keys, _ := redis.Strings(conn.Do("KEYS", "user:*:order"))
	conn.Close()

	count := len(keys)
	orders := make([]Order, count)

	for i := 0; i < count; i++ {
		userId, _ := strconv.Atoi(strings.Split(keys[i], ":")[1])
		items := GetOrderItems(userId)
		orders[i] = GenerateOrder(userId, items)
	}

	return orders
}

