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
	"time"
)

type Queue interface {
	In(string)
	Out() (*Massage, error)
	ReIn(*Massage)
	Remove(string)
}

type queue struct {
	m Mongo
}

const (
	inQ  int = 0
	outQ int = 1
)

type Massage struct {
	ID    string `bson:"_id" json:"_id,omitempty"`
	Data  string `bson:"data" json:"data,omitempty"`
	State int    `bson:"state" json:"state,omitempty"`
	Time  int64  `bson:"timestamp" json:"timestamp,omitempty"`
	Retry int    `bson:"retry" json:"retry,omitempty"`
}

func NewQueue(m Mongo) Queue {
	q := new(queue)
	q.m = m
	return q
}

func (q *queue) In(data string) {
	ma := &Massage{
		ID:    bson.NewObjectId().Hex(),
		Data:  data,
		State: inQ,
		Time:  time.Now().Unix(),
	}
	q.m.Insert(ma)
}

func (q *queue) Out() (*Massage, error) {
	query := bson.M{"state": inQ}
	change := mgo.Change{
		Upsert:    false,
		Remove:    false,
		ReturnNew: false,
		Update:    bson.M{"$set": bson.M{"state": outQ}},
	}
	result, err := q.m.FindAndModifySort("timestamp", query, change)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (q *queue) Remove(id string) {
	q.m.Remove(id)
}

func (q *queue) ReIn(m *Massage) {
	m.Retry = m.Retry + 1
	m.State = inQ

	var addTime time.Duration
	if m.Retry*100 > 500 {
		addTime = 500
	} else {
		addTime = time.Duration(m.Retry) * 100
	}
	m.Time = time.Unix(m.Time, 0).Add(time.Duration(addTime * time.Second)).Unix()
	q.m.Update(m.ID, m)
}
