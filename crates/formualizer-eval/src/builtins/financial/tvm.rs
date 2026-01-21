//! Time Value of Money functions: PMT, PV, FV, NPV, NPER, RATE, IPMT, PPMT

use crate::args::ArgSchema;
use crate::function::Function;
use crate::traits::{ArgumentHandle, CalcValue, FunctionContext};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_macros::func_caps;

fn coerce_num(arg: &ArgumentHandle) -> Result<f64, ExcelError> {
    let v = arg.value()?.into_literal();
    match v {
        LiteralValue::Number(f) => Ok(f),
        LiteralValue::Int(i) => Ok(i as f64),
        LiteralValue::Boolean(b) => Ok(if b { 1.0 } else { 0.0 }),
        LiteralValue::Empty => Ok(0.0),
        LiteralValue::Error(e) => Err(e),
        _ => Err(ExcelError::new_value()),
    }
}

/// PMT(rate, nper, pv, [fv], [type])
/// Calculates the payment for a loan based on constant payments and a constant interest rate
#[derive(Debug)]
pub struct PmtFn;
impl Function for PmtFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "PMT"
    }
    fn min_args(&self) -> usize {
        3
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
            vec![
                ArgSchema::number_lenient_scalar(), // rate
                ArgSchema::number_lenient_scalar(), // nper
                ArgSchema::number_lenient_scalar(), // pv
                ArgSchema::number_lenient_scalar(), // fv (optional)
                ArgSchema::number_lenient_scalar(), // type (optional)
            ]
        });
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        let rate = coerce_num(&args[0])?;
        let nper = coerce_num(&args[1])?;
        let pv = coerce_num(&args[2])?;
        let fv = if args.len() > 3 { coerce_num(&args[3])? } else { 0.0 };
        let pmt_type = if args.len() > 4 { coerce_num(&args[4])? as i32 } else { 0 };

        if nper == 0.0 {
            return Ok(CalcValue::Scalar(LiteralValue::Error(ExcelError::new_num())));
        }

        let pmt = if rate.abs() < 1e-10 {
            // When rate is 0, PMT = -(pv + fv) / nper
            -(pv + fv) / nper
        } else {
            // PMT = (rate * (pv * (1+rate)^nper + fv)) / ((1+rate)^nper - 1)
            // With type adjustment for beginning of period
            let factor = (1.0 + rate).powf(nper);
            let type_adj = if pmt_type != 0 { 1.0 + rate } else { 1.0 };
            -(rate * (pv * factor + fv)) / ((factor - 1.0) * type_adj)
        };

        Ok(CalcValue::Scalar(LiteralValue::Number(pmt)))
    }
}

/// PV(rate, nper, pmt, [fv], [type])
/// Calculates the present value of an investment
#[derive(Debug)]
pub struct PvFn;
impl Function for PvFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "PV"
    }
    fn min_args(&self) -> usize {
        3
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
            vec![
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
            ]
        });
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        let rate = coerce_num(&args[0])?;
        let nper = coerce_num(&args[1])?;
        let pmt = coerce_num(&args[2])?;
        let fv = if args.len() > 3 { coerce_num(&args[3])? } else { 0.0 };
        let pmt_type = if args.len() > 4 { coerce_num(&args[4])? as i32 } else { 0 };

        let pv = if rate.abs() < 1e-10 {
            -fv - pmt * nper
        } else {
            let factor = (1.0 + rate).powf(nper);
            let type_adj = if pmt_type != 0 { 1.0 + rate } else { 1.0 };
            (-fv - pmt * type_adj * (factor - 1.0) / rate) / factor
        };

        Ok(CalcValue::Scalar(LiteralValue::Number(pv)))
    }
}

/// FV(rate, nper, pmt, [pv], [type])
/// Calculates the future value of an investment
#[derive(Debug)]
pub struct FvFn;
impl Function for FvFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "FV"
    }
    fn min_args(&self) -> usize {
        3
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
            vec![
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
            ]
        });
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        let rate = coerce_num(&args[0])?;
        let nper = coerce_num(&args[1])?;
        let pmt = coerce_num(&args[2])?;
        let pv = if args.len() > 3 { coerce_num(&args[3])? } else { 0.0 };
        let pmt_type = if args.len() > 4 { coerce_num(&args[4])? as i32 } else { 0 };

        let fv = if rate.abs() < 1e-10 {
            -pv - pmt * nper
        } else {
            let factor = (1.0 + rate).powf(nper);
            let type_adj = if pmt_type != 0 { 1.0 + rate } else { 1.0 };
            -pv * factor - pmt * type_adj * (factor - 1.0) / rate
        };

        Ok(CalcValue::Scalar(LiteralValue::Number(fv)))
    }
}

/// NPV(rate, value1, [value2], ...)
/// Calculates the net present value of an investment
#[derive(Debug)]
pub struct NpvFn;
impl Function for NpvFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "NPV"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
            vec![ArgSchema::number_lenient_scalar(), ArgSchema::any()]
        });
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        let rate = coerce_num(&args[0])?;

        let mut npv = 0.0;
        let mut period = 1;

        for arg in &args[1..] {
            let v = arg.value()?.into_literal();
            match v {
                LiteralValue::Number(n) => {
                    npv += n / (1.0 + rate).powi(period);
                    period += 1;
                }
                LiteralValue::Int(i) => {
                    npv += (i as f64) / (1.0 + rate).powi(period);
                    period += 1;
                }
                LiteralValue::Error(e) => {
                    return Ok(CalcValue::Scalar(LiteralValue::Error(e)));
                }
                LiteralValue::Array(arr) => {
                    for row in arr {
                        for cell in row {
                            match cell {
                                LiteralValue::Number(n) => {
                                    npv += n / (1.0 + rate).powi(period);
                                    period += 1;
                                }
                                LiteralValue::Int(i) => {
                                    npv += (i as f64) / (1.0 + rate).powi(period);
                                    period += 1;
                                }
                                LiteralValue::Error(e) => {
                                    return Ok(CalcValue::Scalar(LiteralValue::Error(e)));
                                }
                                _ => {} // Skip non-numeric values
                            }
                        }
                    }
                }
                _ => {} // Skip non-numeric values
            }
        }

        Ok(CalcValue::Scalar(LiteralValue::Number(npv)))
    }
}

/// NPER(rate, pmt, pv, [fv], [type])
/// Calculates the number of periods for an investment
#[derive(Debug)]
pub struct NperFn;
impl Function for NperFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "NPER"
    }
    fn min_args(&self) -> usize {
        3
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
            vec![
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
            ]
        });
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        let rate = coerce_num(&args[0])?;
        let pmt = coerce_num(&args[1])?;
        let pv = coerce_num(&args[2])?;
        let fv = if args.len() > 3 { coerce_num(&args[3])? } else { 0.0 };
        let pmt_type = if args.len() > 4 { coerce_num(&args[4])? as i32 } else { 0 };

        let nper = if rate.abs() < 1e-10 {
            if pmt.abs() < 1e-10 {
                return Ok(CalcValue::Scalar(LiteralValue::Error(ExcelError::new_num())));
            }
            -(pv + fv) / pmt
        } else {
            let type_adj = if pmt_type != 0 { 1.0 + rate } else { 1.0 };
            let pmt_adj = pmt * type_adj;
            let numerator = pmt_adj - fv * rate;
            let denominator = pv * rate + pmt_adj;
            if numerator / denominator <= 0.0 {
                return Ok(CalcValue::Scalar(LiteralValue::Error(ExcelError::new_num())));
            }
            (numerator / denominator).ln() / (1.0 + rate).ln()
        };

        Ok(CalcValue::Scalar(LiteralValue::Number(nper)))
    }
}

/// RATE(nper, pmt, pv, [fv], [type], [guess])
/// Calculates the interest rate per period
#[derive(Debug)]
pub struct RateFn;
impl Function for RateFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "RATE"
    }
    fn min_args(&self) -> usize {
        3
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
            vec![
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
            ]
        });
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        let nper = coerce_num(&args[0])?;
        let pmt = coerce_num(&args[1])?;
        let pv = coerce_num(&args[2])?;
        let fv = if args.len() > 3 { coerce_num(&args[3])? } else { 0.0 };
        let pmt_type = if args.len() > 4 { coerce_num(&args[4])? as i32 } else { 0 };
        let guess = if args.len() > 5 { coerce_num(&args[5])? } else { 0.1 };

        // Newton-Raphson iteration to find rate
        let mut rate = guess;
        let max_iter = 100;
        let tolerance = 1e-10;

        for _ in 0..max_iter {
            let type_adj = if pmt_type != 0 { 1.0 + rate } else { 1.0 };

            if rate.abs() < 1e-10 {
                // Special case for very small rate
                let f = pv + pmt * nper + fv;
                if f.abs() < tolerance {
                    return Ok(CalcValue::Scalar(LiteralValue::Number(rate)));
                }
                rate = 0.01; // Nudge away from zero
                continue;
            }

            let factor = (1.0 + rate).powf(nper);
            let f = pv * factor + pmt * type_adj * (factor - 1.0) / rate + fv;

            // Derivative
            let factor_prime = nper * (1.0 + rate).powf(nper - 1.0);
            let df = pv * factor_prime
                + pmt * type_adj * (factor_prime / rate - (factor - 1.0) / (rate * rate));

            if df.abs() < 1e-20 {
                break;
            }

            let new_rate = rate - f / df;

            if (new_rate - rate).abs() < tolerance {
                return Ok(CalcValue::Scalar(LiteralValue::Number(new_rate)));
            }

            rate = new_rate;

            // Prevent rate from going too negative
            if rate < -0.99 {
                rate = -0.99;
            }
        }

        // If we didn't converge, return error
        Ok(CalcValue::Scalar(LiteralValue::Error(ExcelError::new_num())))
    }
}

/// IPMT(rate, per, nper, pv, [fv], [type])
/// Calculates the interest payment for a given period
#[derive(Debug)]
pub struct IpmtFn;
impl Function for IpmtFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "IPMT"
    }
    fn min_args(&self) -> usize {
        4
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
            vec![
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
            ]
        });
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        let rate = coerce_num(&args[0])?;
        let per = coerce_num(&args[1])?;
        let nper = coerce_num(&args[2])?;
        let pv = coerce_num(&args[3])?;
        let fv = if args.len() > 4 { coerce_num(&args[4])? } else { 0.0 };
        let pmt_type = if args.len() > 5 { coerce_num(&args[5])? as i32 } else { 0 };

        if per < 1.0 || per > nper {
            return Ok(CalcValue::Scalar(LiteralValue::Error(ExcelError::new_num())));
        }

        // Calculate PMT first
        let pmt = if rate.abs() < 1e-10 {
            -(pv + fv) / nper
        } else {
            let factor = (1.0 + rate).powf(nper);
            let type_adj = if pmt_type != 0 { 1.0 + rate } else { 1.0 };
            -(rate * (pv * factor + fv)) / ((factor - 1.0) * type_adj)
        };

        // Calculate FV at start of period
        let fv_at_start = if rate.abs() < 1e-10 {
            -pv - pmt * (per - 1.0)
        } else {
            let factor = (1.0 + rate).powf(per - 1.0);
            let type_adj = if pmt_type != 0 { 1.0 + rate } else { 1.0 };
            -pv * factor - pmt * type_adj * (factor - 1.0) / rate
        };

        // Interest is rate * balance at start of period
        let ipmt = if pmt_type != 0 && per == 1.0 {
            0.0 // No interest in first period for annuity due
        } else {
            -fv_at_start * rate
        };

        Ok(CalcValue::Scalar(LiteralValue::Number(ipmt)))
    }
}

/// PPMT(rate, per, nper, pv, [fv], [type])
/// Calculates the principal payment for a given period
#[derive(Debug)]
pub struct PpmtFn;
impl Function for PpmtFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "PPMT"
    }
    fn min_args(&self) -> usize {
        4
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
            vec![
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
            ]
        });
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        let rate = coerce_num(&args[0])?;
        let per = coerce_num(&args[1])?;
        let nper = coerce_num(&args[2])?;
        let pv = coerce_num(&args[3])?;
        let fv = if args.len() > 4 { coerce_num(&args[4])? } else { 0.0 };
        let pmt_type = if args.len() > 5 { coerce_num(&args[5])? as i32 } else { 0 };

        if per < 1.0 || per > nper {
            return Ok(CalcValue::Scalar(LiteralValue::Error(ExcelError::new_num())));
        }

        // Calculate PMT
        let pmt = if rate.abs() < 1e-10 {
            -(pv + fv) / nper
        } else {
            let factor = (1.0 + rate).powf(nper);
            let type_adj = if pmt_type != 0 { 1.0 + rate } else { 1.0 };
            -(rate * (pv * factor + fv)) / ((factor - 1.0) * type_adj)
        };

        // Calculate IPMT
        let fv_at_start = if rate.abs() < 1e-10 {
            -pv - pmt * (per - 1.0)
        } else {
            let factor = (1.0 + rate).powf(per - 1.0);
            let type_adj = if pmt_type != 0 { 1.0 + rate } else { 1.0 };
            -pv * factor - pmt * type_adj * (factor - 1.0) / rate
        };

        let ipmt = if pmt_type != 0 && per == 1.0 {
            0.0
        } else {
            -fv_at_start * rate
        };

        // PPMT = PMT - IPMT
        let ppmt = pmt - ipmt;

        Ok(CalcValue::Scalar(LiteralValue::Number(ppmt)))
    }
}

pub fn register_builtins() {
    use std::sync::Arc;
    crate::function_registry::register_function(Arc::new(PmtFn));
    crate::function_registry::register_function(Arc::new(PvFn));
    crate::function_registry::register_function(Arc::new(FvFn));
    crate::function_registry::register_function(Arc::new(NpvFn));
    crate::function_registry::register_function(Arc::new(NperFn));
    crate::function_registry::register_function(Arc::new(RateFn));
    crate::function_registry::register_function(Arc::new(IpmtFn));
    crate::function_registry::register_function(Arc::new(PpmtFn));
}
