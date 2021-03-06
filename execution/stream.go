//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package execution

import (
	"encoding/json"

	"github.com/couchbase/query/plan"
	"github.com/couchbase/query/value"
)

type Stream struct {
	base
	plan *plan.Stream
}

func NewStream(plan *plan.Stream, context *Context) *Stream {
	rv := &Stream{
		plan: plan,
	}

	newRedirectBase(&rv.base)
	rv.output = rv
	return rv
}

func (this *Stream) Accept(visitor Visitor) (interface{}, error) {
	return visitor.VisitStream(this)
}

func (this *Stream) Copy() Operator {
	rv := &Stream{
		plan: this.plan,
	}
	this.base.copy(&rv.base)
	return rv
}

func (this *Stream) RunOnce(context *Context, parent value.Value) {
	this.runConsumer(this, context, parent)
}

func (this *Stream) processItem(item value.AnnotatedValue, context *Context) bool {
	this.switchPhase(_CHANTIME)
	ok := context.Result(item)
	this.switchPhase(_EXECTIME)
	if ok {
		this.addOutDocs(1)
	}
	return ok
}

func (this *Stream) afterItems(context *Context) {
	context.CloseResults()
}

func (this *Stream) MarshalJSON() ([]byte, error) {
	r := this.plan.MarshalBase(func(r map[string]interface{}) {
		this.marshalTimes(r)
	})
	return json.Marshal(r)
}
