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
	"fmt"
	"math/rand"
	"time"
)

var s rand.Source
var r *rand.Rand

func main() {
	s = rand.NewSource(time.Now().Unix())
	r = rand.New(s)

	m := NewMongoClient("localhost:27017")

	q1 := NewQueue(m)
	q2 := NewQueue(m)

	go in(q1)
	go out(q1, "q1")
	out(q2, "q2")
}

func in(q Queue) {
	defer q.Remove("")
	var i int
	for {
		i++
		data := fmt.Sprintf("Event-%d", i)
		q.In(data)
		println(fmt.Sprintf("#####  queue in: %s", data))

		time.Sleep(time.Duration(50+r.Intn(100)) * time.Millisecond)
	}
}

func out(q Queue, name string) {
	var i int
	defer q.Remove("")
	for {
		i++
		massage, err := q.Out()
		if massage == nil {
			println(fmt.Sprintf("Queue %s: event Outed failed in %d step for reason: %s", name, i, err.Error()))
			time.Sleep(time.Duration(100+r.Intn(100)) * time.Millisecond)
			continue
		}
		if r.Intn(100) > 80 {
			q.ReIn(massage)
			println(fmt.Sprintf("~~~~~Queue %s: event %s ReIn in %d step", name, massage.Data, i))
			time.Sleep(time.Duration(100+r.Intn(100)) * time.Millisecond)
			continue
		}

		println(fmt.Sprintf("Queue %s: event(%s)Outed in %d step", massage.Data, name, i))
		q.Remove(massage.ID)
		time.Sleep(time.Duration(100+r.Intn(100)) * time.Millisecond)
	}
}
