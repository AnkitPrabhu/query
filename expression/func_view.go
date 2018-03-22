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
	NullaryFunctionBase
}

func NewViews() Function {
	rv := &Views{
		*NewNullaryFunctionBase("views"),
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
*/
func (this *Views) Evaluate(item value.Value, context Context) (value.Value, error) {
	return value.NULL_VALUE, nil
}

func (this *Views) Indexable() bool {
	return true
}

func (this *Views) CoveredBy(keyspace string, exprs Expressions, options coveredOptions) Covered {
	if len(this.operands) > 0 {
		alias := NewIdentifier(keyspace)
		if !this.operands[0].DependsOn(alias) {

			// MB-22561: skip the rest of the expression if different keyspace
			return CoveredSkip
		}
	}

	for _, expr := range exprs {
		if this.EquivalentTo(expr) {
			return CoveredTrue
		}
	}

	return CoveredFalse
}

func (this *Views) MinArgs() int { return 0 }

func (this *Views) MaxArgs() int { return 0 }

/*
Factory method pattern.
*/
func (this *Views) Constructor() FunctionConstructor {
	return func(operands ...Expression) Function {
		return NewViews()
	}
}
