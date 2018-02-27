// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::i64;
use std::borrow::Cow;
use super::{Error, EvalContext, FnCall, Result};
use coprocessor::codec::{mysql, Datum};
use coprocessor::codec::mysql::Decimal;

impl FnCall {
    pub fn logical_and(&self, ctx: &EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg0 = self.children[0].eval_int(ctx, row)?;
        if arg0.map_or(false, |v| v == 0) {
            return Ok(Some(0));
        }
        let arg1 = self.children[1].eval_int(ctx, row)?;
        if arg1.map_or(false, |v| v == 0) {
            return Ok(Some(0));
        }
        if arg0.is_none() || arg1.is_none() {
            return Ok(None);
        }
        Ok(Some(1))
    }

    pub fn logical_or(&self, ctx: &EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg0 = self.children[0].eval_int(ctx, row)?;
        if arg0.map_or(false, |v| v != 0) {
            return Ok(Some(1));
        }
        let arg1 = try_opt!(self.children[1].eval_int(ctx, row));
        if arg1 != 0 {
            Ok(Some(1))
        } else {
            Ok(arg0)
        }
    }

    pub fn logical_xor(&self, ctx: &EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg0 = try_opt!(self.children[0].eval_int(ctx, row));
        let arg1 = try_opt!(self.children[1].eval_int(ctx, row));
        Ok(Some(((arg0 == 0) ^ (arg1 == 0)) as i64))
    }

    pub fn int_is_true(&self, ctx: &EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let v = self.children[0].eval_int(ctx, row)?;
        let ret = v.map_or(0, |v| (v != 0) as i64);
        Ok(Some(ret))
    }

    pub fn real_is_true(&self, ctx: &EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let input = self.children[0].eval_real(ctx, row)?;
        Ok(Some(input.map_or(0, |i| (i != 0f64) as i64)))
    }

    pub fn decimal_is_true(&self, ctx: &EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let input = self.children[0].eval_decimal(ctx, row)?;
        Ok(Some(input.map_or(0, |dec| !dec.is_zero() as i64)))
    }

    pub fn int_is_false(&self, ctx: &EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let input = self.children[0].eval_int(ctx, row)?;
        Ok(Some(input.map_or(0, |i| (i == 0) as i64)))
    }

    pub fn real_is_false(&self, ctx: &EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let v = self.children[0].eval_real(ctx, row)?;
        let ret = v.map_or(0, |v| (v == 0f64) as i64);
        Ok(Some(ret))
    }

    pub fn decimal_is_false(&self, ctx: &EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let v = self.children[0].eval_decimal(ctx, row)?;
        let ret = v.map_or(0, |v| v.is_zero() as i64);
        Ok(Some(ret))
    }

    pub fn unary_not(&self, ctx: &EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg = try_opt!(self.children[0].eval_int(ctx, row));
        Ok(Some((arg == 0) as i64))
    }

    pub fn unary_minus_int(&self, ctx: &EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let val = try_opt!(self.children[0].eval_int(ctx, row));
        if mysql::has_unsigned_flag(u64::from(self.children[0].get_tp().get_flag())) {
            let uval = val as u64;
            if uval > i64::MAX as u64 + 1 {
                return Err(Error::Overflow);
            } else if uval == i64::MAX as u64 + 1 {
                return Ok(Some(i64::MIN));
            }
        } else if val == i64::MIN {
            return Err(Error::Overflow);
        }
        Ok(Some(-val))
    }

    pub fn unary_minus_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let dec = try_opt!(self.children[0].eval_decimal(ctx, row)).into_owned();
        Ok(Some(Cow::Owned(-dec)))
    }

    pub fn unary_minus_real(&self, ctx: &EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        let val = try_opt!(self.children[0].eval_real(ctx, row));
        Ok(Some(-val))
    }

    pub fn decimal_is_null(&self, ctx: &EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg = self.children[0].eval_decimal(ctx, row)?;
        Ok(Some(arg.is_none() as i64))
    }

    pub fn int_is_null(&self, ctx: &EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg = self.children[0].eval_int(ctx, row)?;
        Ok(Some(arg.is_none() as i64))
    }

    pub fn real_is_null(&self, ctx: &EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg = self.children[0].eval_real(ctx, row)?;
        Ok(Some(arg.is_none() as i64))
    }

    pub fn string_is_null(&self, ctx: &EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg = self.children[0].eval_string(ctx, row)?;
        Ok(Some(arg.is_none() as i64))
    }

    pub fn time_is_null(&self, ctx: &EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg = self.children[0].eval_time(ctx, row)?;
        Ok(Some(arg.is_none() as i64))
    }

    pub fn duration_is_null(&self, ctx: &EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg = self.children[0].eval_duration(ctx, row)?;
        Ok(Some(arg.is_none() as i64))
    }

    pub fn json_is_null(&self, ctx: &EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg = self.children[0].eval_json(ctx, row)?;
        Ok(Some(arg.is_none() as i64))
    }

    pub fn bit_and(&self, ctx: &EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let lhs = try_opt!(self.children[0].eval_int(ctx, row));
        let rhs = try_opt!(self.children[1].eval_int(ctx, row));
        Ok(Some(lhs & rhs))
    }

    pub fn bit_or(&self, ctx: &EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let lhs = try_opt!(self.children[0].eval_int(ctx, row));
        let rhs = try_opt!(self.children[1].eval_int(ctx, row));
        Ok(Some(lhs | rhs))
    }

    pub fn bit_xor(&self, ctx: &EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let lhs = try_opt!(self.children[0].eval_int(ctx, row));
        let rhs = try_opt!(self.children[1].eval_int(ctx, row));
        Ok(Some(lhs ^ rhs))
    }

    pub fn bit_neg(&self, ctx: &EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let lhs = try_opt!(self.children[0].eval_int(ctx, row));
        Ok(Some(!lhs))
    }
}

#[cfg(test)]
mod test {
    use std::i64;
    use tipb::expression::ScalarFuncSig;
    use coprocessor::codec::Datum;
    use coprocessor::codec::mysql::Duration;
    use coprocessor::dag::expr::{Error, EvalContext, Expression};
    use coprocessor::dag::expr::test::{datum_expr, fncall_expr, str2dec};

    #[test]
    fn test_logic_op() {
        let tests = vec![
            (
                ScalarFuncSig::LogicalAnd,
                Datum::I64(1),
                Datum::I64(1),
                Some(1),
            ),
            (
                ScalarFuncSig::LogicalAnd,
                Datum::I64(1),
                Datum::I64(0),
                Some(0),
            ),
            (
                ScalarFuncSig::LogicalAnd,
                Datum::I64(0),
                Datum::I64(0),
                Some(0),
            ),
            (
                ScalarFuncSig::LogicalAnd,
                Datum::I64(2),
                Datum::I64(-1),
                Some(1),
            ),
            (
                ScalarFuncSig::LogicalAnd,
                Datum::I64(0),
                Datum::Null,
                Some(0),
            ),
            (ScalarFuncSig::LogicalAnd, Datum::Null, Datum::I64(1), None),
            (
                ScalarFuncSig::LogicalOr,
                Datum::I64(1),
                Datum::I64(1),
                Some(1),
            ),
            (
                ScalarFuncSig::LogicalOr,
                Datum::I64(1),
                Datum::I64(0),
                Some(1),
            ),
            (
                ScalarFuncSig::LogicalOr,
                Datum::I64(0),
                Datum::I64(0),
                Some(0),
            ),
            (
                ScalarFuncSig::LogicalOr,
                Datum::I64(2),
                Datum::I64(-1),
                Some(1),
            ),
            (
                ScalarFuncSig::LogicalOr,
                Datum::I64(1),
                Datum::Null,
                Some(1),
            ),
            (ScalarFuncSig::LogicalOr, Datum::Null, Datum::I64(0), None),
            (
                ScalarFuncSig::LogicalXor,
                Datum::I64(1),
                Datum::I64(1),
                Some(0),
            ),
            (
                ScalarFuncSig::LogicalXor,
                Datum::I64(1),
                Datum::I64(0),
                Some(1),
            ),
            (
                ScalarFuncSig::LogicalXor,
                Datum::I64(0),
                Datum::I64(0),
                Some(0),
            ),
            (
                ScalarFuncSig::LogicalXor,
                Datum::I64(2),
                Datum::I64(-1),
                Some(0),
            ),
            (ScalarFuncSig::LogicalXor, Datum::I64(0), Datum::Null, None),
            (ScalarFuncSig::LogicalXor, Datum::Null, Datum::I64(1), None),
        ];
        let ctx = EvalContext::default();
        for (op, lhs, rhs, exp) in tests {
            let arg1 = datum_expr(lhs);
            let arg2 = datum_expr(rhs);
            {
                let op = Expression::build(&ctx, fncall_expr(op, &[arg1.clone(), arg2.clone()]))
                    .unwrap();
                let res = op.eval_int(&ctx, &[]).unwrap();
                assert_eq!(res, exp);
            }
            {
                let op = Expression::build(&ctx, fncall_expr(op, &[arg2, arg1])).unwrap();
                let res = op.eval_int(&ctx, &[]).unwrap();
                assert_eq!(res, exp);
            }
        }
    }

    #[test]
    fn test_unary_minus_op() {
        let tests = vec![
            (
                ScalarFuncSig::UnaryMinusInt,
                Datum::I64(i64::MAX),
                Datum::I64(-i64::MAX),
            ),
            (
                ScalarFuncSig::UnaryMinusInt,
                Datum::I64(-i64::MAX),
                Datum::I64(i64::MAX),
            ),
            (ScalarFuncSig::UnaryMinusInt, Datum::I64(0), Datum::I64(0)),
            (
                ScalarFuncSig::UnaryMinusInt,
                Datum::U64(i64::MAX as u64 + 1),
                Datum::I64(i64::MIN),
            ),
            (ScalarFuncSig::UnaryMinusInt, Datum::Null, Datum::Null),
            (
                ScalarFuncSig::UnaryMinusReal,
                Datum::F64(0.123),
                Datum::F64(-0.123),
            ),
            (
                ScalarFuncSig::UnaryMinusReal,
                Datum::F64(-0.123),
                Datum::F64(0.123),
            ),
            (
                ScalarFuncSig::UnaryMinusReal,
                Datum::F64(0.0),
                Datum::F64(0.0),
            ),
            (ScalarFuncSig::UnaryMinusReal, Datum::Null, Datum::Null),
            (ScalarFuncSig::UnaryMinusDecimal, str2dec("0"), str2dec("0")),
            (
                ScalarFuncSig::UnaryMinusDecimal,
                str2dec("0.123"),
                str2dec("-0.123"),
            ),
            (ScalarFuncSig::UnaryMinusDecimal, Datum::Null, Datum::Null),
        ];
        let ctx = EvalContext::default();
        for (operator, arg, exp) in tests {
            let arg1 = datum_expr(arg);
            let op = Expression::build(&ctx, fncall_expr(operator, &[arg1])).unwrap();
            let res = op.eval(&ctx, &[]).unwrap();
            assert_eq!(res, exp);
        }
    }

    #[test]
    fn test_unary_op() {
        let tests = vec![
            (ScalarFuncSig::UnaryNot, Datum::I64(1), Some(0)),
            (ScalarFuncSig::UnaryNot, Datum::I64(0), Some(1)),
            (ScalarFuncSig::UnaryNot, Datum::I64(123), Some(0)),
            (ScalarFuncSig::UnaryNot, Datum::I64(-123), Some(0)),
            (ScalarFuncSig::UnaryNot, Datum::Null, None),
            (ScalarFuncSig::RealIsTrue, Datum::F64(0.25), Some(1)),
            (ScalarFuncSig::RealIsTrue, Datum::F64(0.0), Some(0)),
            (ScalarFuncSig::RealIsTrue, Datum::Null, Some(0)),
            (ScalarFuncSig::RealIsFalse, Datum::F64(0.25), Some(0)),
            (ScalarFuncSig::RealIsFalse, Datum::F64(0.000), Some(1)),
            (ScalarFuncSig::RealIsFalse, Datum::F64(-0.00), Some(1)),
            (ScalarFuncSig::RealIsFalse, Datum::F64(-0.011), Some(0)),
            (ScalarFuncSig::RealIsFalse, Datum::Null, Some(0)),
            (ScalarFuncSig::RealIsNull, Datum::F64(1.25), Some(0)),
            (ScalarFuncSig::RealIsNull, Datum::Null, Some(1)),
            (ScalarFuncSig::DecimalIsTrue, str2dec("1.1"), Some(1)),
            (ScalarFuncSig::DecimalIsTrue, str2dec("0"), Some(0)),
            (ScalarFuncSig::DecimalIsTrue, Datum::Null, Some(0)),
            (ScalarFuncSig::DecimalIsFalse, str2dec("1.1"), Some(0)),
            (ScalarFuncSig::DecimalIsFalse, str2dec("0"), Some(1)),
            (ScalarFuncSig::DecimalIsFalse, Datum::Null, Some(0)),
            (ScalarFuncSig::DecimalIsNull, str2dec("1.1"), Some(0)),
            (ScalarFuncSig::DecimalIsNull, Datum::Null, Some(1)),
            (ScalarFuncSig::IntIsTrue, Datum::I64(0), Some(0)),
            (ScalarFuncSig::IntIsTrue, Datum::I64(12), Some(1)),
            (ScalarFuncSig::IntIsTrue, Datum::Null, Some(0)),
            (ScalarFuncSig::IntIsFalse, Datum::I64(0), Some(1)),
            (ScalarFuncSig::IntIsFalse, Datum::I64(1), Some(0)),
            (ScalarFuncSig::IntIsFalse, Datum::Null, Some(0)),
            (ScalarFuncSig::IntIsNull, Datum::I64(1), Some(0)),
            (ScalarFuncSig::IntIsNull, Datum::Null, Some(1)),
            (ScalarFuncSig::StringIsNull, Datum::Null, Some(1)),
            (
                ScalarFuncSig::StringIsNull,
                Datum::Bytes(b"abc".to_vec()),
                Some(0),
            ),
            (ScalarFuncSig::TimeIsNull, Datum::Null, Some(1)),
            // TODO: add Time related tests after Time is implemented in Expression::build
            (
                ScalarFuncSig::DurationIsNull,
                Datum::Dur(Duration::zero()),
                Some(0),
            ),
            (ScalarFuncSig::DurationIsNull, Datum::Null, Some(1)),
        ];
        let ctx = EvalContext::default();
        for (op, arg, exp) in tests {
            let arg1 = datum_expr(arg);
            let op = Expression::build(&ctx, fncall_expr(op, &[arg1])).unwrap();
            let res = op.eval_int(&ctx, &[]).unwrap();
            assert_eq!(res, exp);
        }
    }

    fn check_overflow(e: Error) -> Result<(), ()> {
        match e {
            Error::Overflow => Ok(()),
            _ => Err(()),
        }
    }

    #[test]
    fn test_unary_op_overflow() {
        let tests = vec![
            (ScalarFuncSig::UnaryMinusInt, Datum::I64(i64::MIN)),
            (
                ScalarFuncSig::UnaryMinusInt,
                Datum::U64(i64::MAX as u64 + 2),
            ),
        ];
        let ctx = EvalContext::default();
        for (op, argument) in tests {
            let arg = datum_expr(argument);
            let op = Expression::build(&ctx, fncall_expr(op, &[arg])).unwrap();
            let got = op.eval(&ctx, &[]).unwrap_err();
            assert!(check_overflow(got).is_ok());
        }
    }

    #[test]
    fn test_bit_and() {
        let cases = vec![
            (Datum::I64(123), Datum::I64(321), Datum::I64(65)),
            (Datum::I64(-123), Datum::I64(321), Datum::I64(257)),
            (Datum::Null, Datum::I64(1), Datum::Null),
        ];
        let ctx = EvalContext::default();
        for (lhs, rhs, exp) in cases {
            let args = &[datum_expr(lhs), datum_expr(rhs)];
            let op = Expression::build(&ctx, fncall_expr(ScalarFuncSig::BitAndSig, args)).unwrap();
            let res = op.eval(&ctx, &[]).unwrap();
            assert_eq!(res, exp);
        }
    }

    #[test]
    fn test_bit_or() {
        let cases = vec![
            (Datum::I64(123), Datum::I64(321), Datum::I64(379)),
            (Datum::I64(-123), Datum::I64(321), Datum::I64(-59)),
            (Datum::Null, Datum::I64(1), Datum::Null),
        ];
        let ctx = EvalContext::default();
        for (lhs, rhs, exp) in cases {
            let args = &[datum_expr(lhs), datum_expr(rhs)];
            let op = Expression::build(&ctx, fncall_expr(ScalarFuncSig::BitOrSig, args)).unwrap();
            let res = op.eval(&ctx, &[]).unwrap();
            assert_eq!(res, exp);
        }
    }

    #[test]
    fn test_bit_xor() {
        let cases = vec![
            (Datum::I64(123), Datum::I64(321), Datum::I64(314)),
            (Datum::I64(-123), Datum::I64(321), Datum::I64(-316)),
            (Datum::Null, Datum::I64(1), Datum::Null),
        ];
        let ctx = EvalContext::default();
        for (lhs, rhs, exp) in cases {
            let args = &[datum_expr(lhs), datum_expr(rhs)];
            let op = Expression::build(&ctx, fncall_expr(ScalarFuncSig::BitXorSig, args)).unwrap();
            let res = op.eval(&ctx, &[]).unwrap();
            assert_eq!(res, exp);
        }
    }

    #[test]
    fn test_bit_neg() {
        let cases = vec![
            (Datum::I64(123), Datum::I64(-124)),
            (Datum::I64(-123), Datum::I64(122)),
            (Datum::Null, Datum::Null),
        ];
        let ctx = EvalContext::default();
        for (arg, exp) in cases {
            let args = &[datum_expr(arg)];
            let op = Expression::build(&ctx, fncall_expr(ScalarFuncSig::BitNegSig, args)).unwrap();
            let res = op.eval(&ctx, &[]).unwrap();
            assert_eq!(res, exp);
        }
    }
}
