package sql

import (
    "fmt"
    _ "github.com/go-sql-driver/mysql"
    "testing"
)

func TestStdSql(t *testing.T) {
    s := NewSql()
    _, err := s.Connect("mysql", "root:123456@tcp(127.0.0.1:3306)/test")
    if err != nil {
        fmt.Println(err)
        return
    }
    defer s.Close()
    _, err = s.Execute("use test")
    if err != nil {
        fmt.Println(err)
        return
    }
    _, err = s.Execute("select * from t_user_info;")
    if err != nil {
        fmt.Println(err)
        return
    }
}
