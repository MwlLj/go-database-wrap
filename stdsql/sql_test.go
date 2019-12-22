package stdsql

import (
    "testing"
    _ "github.com/go-sql-driver/mysql"
    "fmt"
)

type queryExtra struct {
    F1 *string `json:"f1"`
}

type queryResult struct {
    Name *string `field:"name"`
    Age *int `field:"age"`
    Sex *string `field:"sex"`
    Extra *queryExtra `field:"extra" type:"json"`
}

func TestQuery(t *testing.T) {
    s := NewSql()
    _, err := s.Connect("mysql", "root:123456@tcp(127.0.0.1:3306)/test")
    if err != nil {
        fmt.Println("connect: ", err)
        return
    }
    defer s.Close()
    r := queryResult{}
    err = s.QueryByTag(&r, "select * from t_user_info;")
    if err != nil {
        fmt.Println("query: ", err)
        return
    }
    if r.Name != nil {
        fmt.Printf("name: %s\n", *r.Name)
    }
    if r.Age != nil {
        fmt.Printf("age: %d\n", *r.Age)
    }
    if r.Sex != nil {
        fmt.Printf("sex: %s\n", *r.Sex)
    }
    if r.Extra != nil {
        if r.Extra.F1 != nil {
            fmt.Printf("extra.F1: %s\n", *r.Extra.F1)
        } else {
            fmt.Println("f1 = nil")
        }
    } else {
        fmt.Println("extra == nil")
    }
}
