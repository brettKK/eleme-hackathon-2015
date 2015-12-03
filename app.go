package main

import (
	"github.com/julienschmidt/httprouter"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"
	"strconv"
	"runtime"
	"bytes"
	"log"
	"fmt"
	"os"

	"./model"
)


// Errors

type ErrorNum int
const (
	INVALID_ACCESS_TOKEN ErrorNum = iota
	EMPTY_REQUEST
	MALFORMED_JSON
	USER_AUTH_FAIL
	CART_NOT_FOUND
	NOT_AUTHORIZED_TO_ACCESS_CART
	FOOD_NOT_FOUND
	FOOD_OUT_OF_LIMIT
	ORDER_OUT_OF_LIMIT
	FOOD_OUT_OF_STOCK
)

type ErrorInfo struct {
	status int
	message string
}
var errorList = []ErrorInfo {
	{401, `{"code": "INVALID_ACCESS_TOKEN", "message": "无效的令牌"}`},
	{400, `{"code": "EMPTY_REQUEST", "message": "请求体为空"}`},
	{400, `{"code": "MALFORMED_JSON", "message": "格式错误"}`},
	{403, `{"code": "USER_AUTH_FAIL", "message": "用户名或密码错误"}`},
	{404, `{"code": "CART_NOT_FOUND", "message": "篮子不存在"}`},
	{401, `{"code": "NOT_AUTHORIZED_TO_ACCESS_CART", "message": "无权限访问指定的篮子"}`},
	{404, `{"code": "FOOD_NOT_FOUND", "message": "食物不存在"}`},
	{403, `{"code": "FOOD_OUT_OF_LIMIT", "message": "篮子中食物数量超过了三个"}`},
	{403, `{"code": "ORDER_OUT_OF_LIMIT", "message": "每个用户只能下一单"}`},
	{403, `{"code": "FOOD_OUT_OF_STOCK", "message": "食物库存不足"}`},
}

func RespondError(rw http.ResponseWriter, num ErrorNum) {
	info := errorList[num]
	rw.WriteHeader(info.status)
	rw.Write([]byte(info.message))
}


// Message

func RespondObject(rw http.ResponseWriter, obj interface{}) {
	objStr, _ := json.Marshal(obj)
	rw.Write(objStr)
}


// Context

type Context struct {
	userId int
	data map[string]interface{}
	params httprouter.Params
}

type ContextHandle func (http.ResponseWriter, *http.Request, Context)

// Middlewares

func IdDecode(uid string) int {
	bstr := []byte(uid)
	id := 0
	for i := 0; i < 5; i++ {
		id = id * 10 + int(bstr[i] - byte('A'))
	}
	return id
}

func CheckToken(handle ContextHandle) httprouter.Handle {
	return func (rw http.ResponseWriter, req *http.Request, ps httprouter.Params) {
		token := req.Header.Get("Access-Token")
		if len(token) == 0 {
			token = req.URL.Query().Get("access_token")
		}
		if len(token) <= 5 {
			RespondError(rw, INVALID_ACCESS_TOKEN)
			return
		}

		userId := IdDecode(token[:5])
		if tk, ok := model.TokenCache[userId]; !ok || tk != token {
			RespondError(rw, INVALID_ACCESS_TOKEN)
			return
		}

		c := Context{
			userId: userId,
			params: ps,
		}

		handle(rw, req, c)
	}
}

func CheckJson(handle ContextHandle) httprouter.Handle {
	return func (rw http.ResponseWriter, req *http.Request, ps httprouter.Params) {
		c := Context{}

		raw, err := ioutil.ReadAll(req.Body)
		if err != nil || len(raw) == 0 {
			RespondError(rw, EMPTY_REQUEST)
			return
		}

		if err := json.Unmarshal(raw, &(c.data)); err != nil {
			RespondError(rw, MALFORMED_JSON)
			return
		}

		handle(rw, req, c)
	}
}

func CheckTokenJson(handle ContextHandle) httprouter.Handle {
	return func (rw http.ResponseWriter, req *http.Request, ps httprouter.Params) {
		token := req.Header.Get("Access-Token")
		if len(token) == 0 {
			token = req.URL.Query().Get("access_token")
		}
		if len(token) <= 5 {
			RespondError(rw, INVALID_ACCESS_TOKEN)
			return
		}

		userId := IdDecode(token[:5])
		if tk, ok := model.TokenCache[userId]; !ok || tk != token {
			RespondError(rw, INVALID_ACCESS_TOKEN)
			return
		}

		c := Context{
			userId: userId,
			params: ps,
		}

		raw, err := ioutil.ReadAll(req.Body)
		if err != nil || len(raw) == 0 {
			RespondError(rw, EMPTY_REQUEST)
			return
		}

		if err := json.Unmarshal(raw, &(c.data)); err != nil {
			RespondError(rw, MALFORMED_JSON)
			return
		}

		handle(rw, req, c)
	}
}


// Handlers

func Login(rw http.ResponseWriter, req *http.Request, c Context) {
	userName := c.data["username"].(string)
	passWord := c.data["password"].(string)

	user, ok := model.UserCache[userName]
	if !ok || user.Pwd != passWord {
		RespondError(rw, USER_AUTH_FAIL)
		return
	}

	var ret bytes.Buffer
	ret.WriteString(`{"user_id":`)
	ret.WriteString(strconv.Itoa(user.Id))
	ret.WriteString(`,"username":"`)
	ret.WriteString(userName)
	ret.WriteString(`","access_token":"`)
	ret.WriteString(model.TokenCache[user.Id])
	ret.WriteString(`"}`)
	ret.WriteTo(rw)
}

func FetchFoods(rw http.ResponseWriter, req *http.Request, c Context) {
	rw.Write(model.FoodsString)
}

func CreateCart(rw http.ResponseWriter, req *http.Request, c Context) {
	cartId := model.CreateCart(c.userId)
	var ret bytes.Buffer
	ret.WriteString(`{"cart_id":"`)
	ret.WriteString(cartId)
	ret.WriteString(`"}`)
	ret.WriteTo(rw)
}

func AddFood(rw http.ResponseWriter, req *http.Request, c Context) {
	foodId := int(c.data["food_id"].(float64))
	if foodId < 1 || foodId > 100 {
		RespondError(rw, FOOD_NOT_FOUND)
		return
	}
	foodCount := int(c.data["count"].(float64))
	if foodCount == 0 {
		rw.WriteHeader(204)
		return
	}
	if foodCount > 3 {
		RespondError(rw, FOOD_OUT_OF_LIMIT)
		return
	}

	cartId := c.params[0].Value

	cartIdApart := strings.SplitN(cartId, "_", 2)
	if len(cartIdApart) != 2 {
		RespondError(rw, CART_NOT_FOUND)
		return
	}

	userId, _ := strconv.Atoi(cartIdApart[0])
	if userId != c.userId {
		RespondError(rw, NOT_AUTHORIZED_TO_ACCESS_CART)
		return
	}

	if !model.AddFoodCount(cartId, foodId, foodCount) {
		RespondError(rw, FOOD_OUT_OF_LIMIT)
		return
	}

	rw.WriteHeader(204)
}

func MakeOrder(rw http.ResponseWriter, req *http.Request, c Context) {
	cartId := c.data["cart_id"].(string)
	cartIdApart := strings.SplitN(cartId, "_", 2)
	if len(cartIdApart) != 2 {
		RespondError(rw, CART_NOT_FOUND)
		return
	}
	userId, _ := strconv.Atoi(cartIdApart[0])
	if userId != c.userId {
		RespondError(rw, NOT_AUTHORIZED_TO_ACCESS_CART)
		return
	}

	result := model.TryMakeOrder(c.userId, cartId)
	switch result {
	case -1:
		RespondError(rw, ORDER_OUT_OF_LIMIT)
		return
	case -2:
		RespondError(rw, FOOD_OUT_OF_STOCK)
		return
	default:
		var ret bytes.Buffer
		ret.WriteString(`{"id":"`)
		ret.WriteString(strconv.Itoa(userId))
		ret.WriteString(`"}`)
		ret.WriteTo(rw)
		return
	}
}

func FetchOrder(rw http.ResponseWriter, req *http.Request, c Context) {
	items := model.GetOrderItems(c.userId)
	if items == nil {
		rw.Write([]byte("[]"))
		return
	}

	order := model.GenerateOrder(c.userId, items)
	orders := []model.Order{order}
	RespondObject(rw, orders)
}

func FetchAdminOrders(rw http.ResponseWriter, req *http.Request, c Context) {
	if c.userId != 1 {
		rw.Write([]byte("[]"))
		return
	}

	orders := model.GetAllOrders()
	RespondObject(rw, orders)
}


// Server

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	mysqlDSN := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
							os.Getenv("DB_USER"),
							os.Getenv("DB_PASS"),
							os.Getenv("DB_HOST"),
							os.Getenv("DB_PORT"),
							os.Getenv("DB_NAME"))
	redisDSN := fmt.Sprintf("%s:%s", os.Getenv("REDIS_HOST"), os.Getenv("REDIS_PORT"))

	model.InitDatabases(mysqlDSN, redisDSN)

	model.InitAllCache()

    router := httprouter.New()

	router.POST("/login", CheckJson(Login))

	router.GET("/foods", CheckToken(FetchFoods))

	router.POST("/carts", CheckToken(CreateCart))
	router.PATCH("/carts/:cart_id", CheckTokenJson(AddFood))

	router.POST("/orders", CheckTokenJson(MakeOrder))
	router.GET("/orders", CheckToken(FetchOrder))
	router.GET("/admin/orders", CheckToken(FetchAdminOrders))

	serverDSN := fmt.Sprintf("%s:%s", os.Getenv("APP_HOST"), os.Getenv("APP_PORT"))

	log.Fatal(http.ListenAndServe(serverDSN, router))
}
