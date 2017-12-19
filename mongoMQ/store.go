/*
Copyright 2016 Xugang Liang. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "LICENSE");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"fmt"
)

const (
	MQDB string = "massageQueue"
	MQC  string = "massages"
)

type Mongo interface {
	Insert(interface{}) error
	Update(string, *Massage) error
	FindAndModifySort(string, bson.M, mgo.Change) (*Massage, error)
	Remove(string) error
	Close()
}

type mongo struct {
	s *mgo.Session
	c *mgo.Collection
}

// NewMongoClient return a mongo client setting up mongo items in test
func NewMongoClient(host string) Mongo {
	session, err := mgo.Dial(host)
	if err != nil {
		return nil
	}
	session.SetMode(mgo.Eventual, true)

	m := new(mongo)

	m.s = session

	m.c = session.DB(MQDB).C(MQC)

	return m
}

// Insert inserts item
func (m *mongo) Insert(item interface{}) error {
	return m.c.Insert(item)
}

func (m *mongo) FindAndModifySort(sort string, query bson.M, change mgo.Change) (*Massage, error) {
	result := &Massage{}
	changeInfo, err := m.c.Find(query).Sort(sort).Apply(change, result)
	if err != nil {
		return nil, err
	}

	if changeInfo.Matched > 1 || changeInfo.Updated > 1 {
		return nil, fmt.Errorf("more than 1 same events in queue")
	}

	return result, nil
}

// Update updates the item named $name
func (m *mongo) Update(id string, item *Massage) error {
	query := bson.M{"_id": id}

	count, err := m.c.Find(query).Count()
	if err != nil {
		return err
	}

	if count == 0 {
		return mgo.ErrNotFound
	} else if count > 1 {
		return fmt.Errorf("there are %d items with the same id %s", count, id)
	}

	return m.c.Update(query, item)
}

// Remove removes item named $name
// Note that if $name == "", Remove will remove all items in the collection
func (m *mongo) clear() error {
	return m.Remove("")
}

func (m *mongo) Remove(id string) error {
	if id == "" {
		_, err := m.c.RemoveAll(nil)
		return err
	}

	query := bson.M{"_id": id}
	return m.c.Remove(query)
}

func (m *mongo) Close() {
	m.clear()
	m.s.Close()
}
