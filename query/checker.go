package query

import (
	"errors"
	"fmt"
)

func (e *BinaryOpExpr) Check() error {
	switch e.Op {
	case And, Or:
		return e.checkWithAndOr()
	case Not:
		return errors.New("Syntax Error: Invalid operator !")
	default:
		return e.checkWithCompares()
	}
}

func (e *BinaryOpExpr) checkWithAndOr() error {
	op := OperatorToString[e.Op]
	switch exp := e.Left.(type) {
	case *BinaryOpExpr, *FunctionCallExpr, *NotExpr:
		// Correct expressions
	default:
		return fmt.Errorf("Syntax Error: %s operator with invalid left expression %s", op, exp)
	}

	switch exp := e.Right.(type) {
	case *BinaryOpExpr, *FunctionCallExpr, *NotExpr:
		// Correct expressions
	default:
		return fmt.Errorf("Syntax Error: %s operator with invalid right expression %s", op, exp)
	}
	return nil
}

func (e *BinaryOpExpr) checkWithCompares() error {
	var (
		numKeyFieldExpr   = 0
		numValueFieldExpr = 0
		numCallExpr       = 0
	)
	op := OperatorToString[e.Op]

	switch exp := e.Left.(type) {
	case *FieldExpr:
		switch exp.Field {
		case KeyKW:
			numKeyFieldExpr++
		case ValueKW:
			numValueFieldExpr++
		}
	case *FunctionCallExpr:
		numCallExpr++
	case *StringExpr:
	default:
		return fmt.Errorf("Syntax Error: %s operator with invalid left expression", op)
	}

	switch exp := e.Right.(type) {
	case *FieldExpr:
		switch exp.Field {
		case KeyKW:
			numKeyFieldExpr++
		case ValueKW:
			numValueFieldExpr++
		}
	case *FunctionCallExpr:
		numCallExpr++
	case *StringExpr:
	default:
		return fmt.Errorf("Syntax Error: %s operator with invalid right expression", op)
	}
	if numKeyFieldExpr == 2 || numValueFieldExpr == 2 {
		return fmt.Errorf("Syntax Error: %s operator with two same field", op)
	}
	if numKeyFieldExpr == 0 && numValueFieldExpr == 0 && numCallExpr == 0 {
		return fmt.Errorf("Syntax Error: %s operator with no field nor function call", op)
	}
	return nil
}

func (e *FieldExpr) Check() error {
	return nil
}

func (e *StringExpr) Check() error {
	return nil
}

func (e *NotExpr) Check() error {
	_, ok := e.Right.(*BinaryOpExpr)
	if !ok {
		return errors.New("Syntax Error: ! operator followed with invalid expression")
	}
	return nil
}

func (e *FunctionCallExpr) Check() error {
	_, ok := e.Name.(*NameExpr)
	if !ok {
		return errors.New("Syntax Error: Invalid function name")
	}
	return nil
}

func (e *NameExpr) Check() error {
	return nil
}

func (e *FloatExpr) Check() error {
	return nil
}

func (e *NumberExpr) Check() error {
	return nil
}
