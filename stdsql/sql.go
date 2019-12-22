package stdsql

import (
    "database/sql"
    "errors"
    ser "github.com/MwlLj/go-serde/stdsql_serde"
)

type CSql struct {
    db *sql.DB
}

/*
** MaxOpenConnsDefault: 20
** MaxIdleConnsDefault: 10
** ConnMaxLifetimeDefault: 0
*/
func (self *CSql) Connect(driverName string, dial string) (*sql.DB, error) {
    var err error = nil
    self.db, err = sql.Open(driverName, dial)
    self.db.SetMaxOpenConns(20)
    self.db.SetMaxIdleConns(10)
    return self.db, err
}

func (self *CSql) Close() error {
    if self.db != nil {
        return self.db.Close()
    } else {
        return errors.New("db is closed")
    }
    return nil
}

func (self *CSql) ExecuteWithTranslate(query string, args ...interface{}) (*sql.Result, error) {
    if self.db == nil {
        return nil, errors.New("db is closed")
    }
    tx, err := self.db.Begin()
    if err != nil {
        return nil, err
    }
    result, err := tx.Exec(query, args...)
    if err != nil {
        tx.Rollback()
        return nil, err
    }
    tx.Commit()
    return &result, nil
}

func (self *CSql) Execute(tx *sql.Tx, query string, args ...interface{}) (*sql.Result, error) {
    if self.db == nil {
        return nil, errors.New("db is closed")
    }
    result, err := tx.Exec(query, args...)
    if err != nil {
        return nil, err
    }
    return &result, err
}

func (self *CSql) Query(query string, args ...interface{}) (*sql.Rows, error) {
    if self.db == nil {
        return nil, errors.New("db is closed")
    }
    rows, err := self.db.Query(query, args...)
    if err != nil {
        return nil, err
    }
    return rows, nil
}

func (self *CSql) QueryByTag(output interface{}, query string, args ...interface{}) error {
    rows, err := self.Query(query, args...)
    if err != nil {
        return err
    }
    defer rows.Close()
    err = ser.ByTag(rows, output)
    if err != nil {
        return err
    }
    return nil
}

func NewSql() *CSql {
    s := CSql{}
    return &s
}
