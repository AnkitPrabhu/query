//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package expression

import (
	"log"

	"github.com/couchbase/query/value"
)

///////////////////////////////////////////////////
//
// View
//
///////////////////////////////////////////////////

/*
This represents the Meta function Views().
It instructs GSI to use JSEvaluator.
*/
type Views struct {
	FunctionBase
}

func NewViews(operands ...Expression) Function {
	rv := &Views{
		*NewFunctionBase("views", operands...),
	}

	rv.expr = rv
	return rv
}

/*
Visitor pattern.
*/
func (this *Views) Accept(visitor Visitor) (interface{}, error) {
	return visitor.VisitFunction(this)
}

func (this *Views) Type() value.Type { return value.OBJECT }

/*
Dummy function for Views

func (this *Views) Evaluate(item value.Value, context Context) (value.Value, error) {
	return value.NULL_VALUE, nil
}
*/
func (this *Views) Indexable() bool {
	return true
}

func (this *Views) Evaluate(item value.Value, context Context) (value.Value, error) {
	val := item

	if len(this.operands) > 0 {
		arg, err := this.operands[0].Evaluate(item, context)
		if err != nil {
			return nil, err
		}

		val = arg
	}

	if val.Type() == value.MISSING {
		return val, nil
	}

	switch val := val.(type) {
	case value.AnnotatedValue:
		return value.NewValue(val.GetAttachment("meta")), nil
	default:
		return value.NULL_VALUE, nil
	}
}

func (this *Views) CoveredBy(keyspace string, exprs Expressions, options coveredOptions) Covered {
	log.Printf("DBG: CoveredBy, keyspace: %s \n exprs:%v \n options:%v \n operands:%v", keyspace, exprs, options, this.operands)
	if len(this.operands) > 0 {
		alias := NewIdentifier(keyspace)
		if !this.operands[0].DependsOn(alias) {

			// MB-22561: skip the rest of the expression if different keyspace
			log.Printf("DBG: CoveredBy returning CoveredSkip")
			return CoveredSkip
		}
	}

	for _, expr := range exprs {
		if this.EquivalentTo(expr) {
			log.Printf("DBG: CoveredBy returning CoveredTrue")
			return CoveredTrue
		}
	}

	log.Printf("DBG: CoveredBy returning CoveredFalse")
	return CoveredFalse
}

func (this *Views) MinArgs() int { return 0 }

func (this *Views) MaxArgs() int { return 0 }

/*
Factory method pattern.
*/
func (this *Views) Constructor() FunctionConstructor {
	return NewViews
}
