//! Date and time component extraction functions

use super::serial::{serial_to_date, serial_to_datetime};
use crate::args::ArgSchema;
use crate::function::Function;
use crate::traits::{ArgumentHandle, FunctionContext};
use chrono::{Datelike, Timelike};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_macros::func_caps;

fn coerce_to_serial(arg: &ArgumentHandle) -> Result<f64, ExcelError> {
    let v = arg.value()?.into_literal();
    match v {
        LiteralValue::Number(f) => Ok(f),
        LiteralValue::Int(i) => Ok(i as f64),
        LiteralValue::Text(s) => s.parse::<f64>().map_err(|_| {
            ExcelError::new_value().with_message("Date/time serial is not a valid number")
        }),
        LiteralValue::Boolean(b) => Ok(if b { 1.0 } else { 0.0 }),
        LiteralValue::Empty => Ok(0.0),
        LiteralValue::Error(e) => Err(e),
        _ => Err(ExcelError::new_value()
            .with_message("Date/time functions expect numeric or text-numeric serials")),
    }
}

/// YEAR(serial_number) - Extracts year from date
#[derive(Debug)]
pub struct YearFn;

impl Function for YearFn {
    func_caps!(PURE);

    fn name(&self) -> &'static str {
        "YEAR"
    }

    fn min_args(&self) -> usize {
        1
    }

    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static ONE: LazyLock<Vec<ArgSchema>> =
            LazyLock::new(|| vec![ArgSchema::number_lenient_scalar()]);
        &ONE[..]
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let serial = coerce_to_serial(&args[0])?;
        let date = serial_to_date(serial)?;
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(
            date.year() as i64,
        )))
    }
}

/// MONTH(serial_number) - Extracts month from date
#[derive(Debug)]
pub struct MonthFn;

impl Function for MonthFn {
    func_caps!(PURE);

    fn name(&self) -> &'static str {
        "MONTH"
    }

    fn min_args(&self) -> usize {
        1
    }

    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static ONE: LazyLock<Vec<ArgSchema>> =
            LazyLock::new(|| vec![ArgSchema::number_lenient_scalar()]);
        &ONE[..]
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let serial = coerce_to_serial(&args[0])?;
        let date = serial_to_date(serial)?;
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(
            date.month() as i64,
        )))
    }
}

/// DAY(serial_number) - Extracts day from date
#[derive(Debug)]
pub struct DayFn;

impl Function for DayFn {
    func_caps!(PURE);

    fn name(&self) -> &'static str {
        "DAY"
    }

    fn min_args(&self) -> usize {
        1
    }

    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static ONE: LazyLock<Vec<ArgSchema>> =
            LazyLock::new(|| vec![ArgSchema::number_lenient_scalar()]);
        &ONE[..]
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let serial = coerce_to_serial(&args[0])?;
        let date = serial_to_date(serial)?;
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(
            date.day() as i64,
        )))
    }
}

/// HOUR(serial_number) - Extracts hour from time
#[derive(Debug)]
pub struct HourFn;

impl Function for HourFn {
    func_caps!(PURE);

    fn name(&self) -> &'static str {
        "HOUR"
    }

    fn min_args(&self) -> usize {
        1
    }

    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static ONE: LazyLock<Vec<ArgSchema>> =
            LazyLock::new(|| vec![ArgSchema::number_lenient_scalar()]);
        &ONE[..]
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let serial = coerce_to_serial(&args[0])?;

        // For time values < 1, we just use the fractional part
        let time_fraction = if serial < 1.0 { serial } else { serial.fract() };

        // Convert fraction to hours (24 hours = 1.0)
        let hours = (time_fraction * 24.0) as i64;
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(hours)))
    }
}

/// MINUTE(serial_number) - Extracts minute from time
#[derive(Debug)]
pub struct MinuteFn;

impl Function for MinuteFn {
    func_caps!(PURE);

    fn name(&self) -> &'static str {
        "MINUTE"
    }

    fn min_args(&self) -> usize {
        1
    }

    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static ONE: LazyLock<Vec<ArgSchema>> =
            LazyLock::new(|| vec![ArgSchema::number_lenient_scalar()]);
        &ONE[..]
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let serial = coerce_to_serial(&args[0])?;

        // Extract time component
        let datetime = serial_to_datetime(serial)?;
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(
            datetime.minute() as i64,
        )))
    }
}

/// SECOND(serial_number) - Extracts second from time
#[derive(Debug)]
pub struct SecondFn;

impl Function for SecondFn {
    func_caps!(PURE);

    fn name(&self) -> &'static str {
        "SECOND"
    }

    fn min_args(&self) -> usize {
        1
    }

    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static ONE: LazyLock<Vec<ArgSchema>> =
            LazyLock::new(|| vec![ArgSchema::number_lenient_scalar()]);
        &ONE[..]
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let serial = coerce_to_serial(&args[0])?;

        // Extract time component
        let datetime = serial_to_datetime(serial)?;
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(
            datetime.second() as i64,
        )))
    }
}

/// DAYS(end_date, start_date) - Returns the number of days between two dates
#[derive(Debug)]
pub struct DaysFn;

impl Function for DaysFn {
    func_caps!(PURE);

    fn name(&self) -> &'static str {
        "DAYS"
    }

    fn min_args(&self) -> usize {
        2
    }

    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static TWO: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
            vec![
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
            ]
        });
        &TWO[..]
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let end_serial = coerce_to_serial(&args[0])?;
        let start_serial = coerce_to_serial(&args[1])?;
        let days = (end_serial.trunc() - start_serial.trunc()) as i64;
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(days)))
    }
}

/// DAYS360(start_date, end_date, [method]) - Returns number of days using 360-day year
#[derive(Debug)]
pub struct Days360Fn;

impl Function for Days360Fn {
    func_caps!(PURE);

    fn name(&self) -> &'static str {
        "DAYS360"
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
            vec![
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
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let start_serial = coerce_to_serial(&args[0])?;
        let end_serial = coerce_to_serial(&args[1])?;
        let european = if args.len() > 2 {
            let v = args[2].value()?.into_literal();
            match v {
                LiteralValue::Boolean(b) => b,
                LiteralValue::Number(n) => n != 0.0,
                LiteralValue::Int(i) => i != 0,
                _ => false,
            }
        } else {
            false
        };

        let start_date = serial_to_date(start_serial)?;
        let end_date = serial_to_date(end_serial)?;

        let mut start_day = start_date.day() as i32;
        let mut start_month = start_date.month() as i32;
        let start_year = start_date.year();

        let mut end_day = end_date.day() as i32;
        let mut end_month = end_date.month() as i32;
        let end_year = end_date.year();

        if european {
            // European method: If day is 31, change to 30
            if start_day == 31 {
                start_day = 30;
            }
            if end_day == 31 {
                end_day = 30;
            }
        } else {
            // US/NASD method
            // Check if start_date is last day of February
            let start_is_last_feb = start_month == 2 && is_last_day_of_month(&start_date);
            let end_is_last_feb = end_month == 2 && is_last_day_of_month(&end_date);

            if start_is_last_feb && end_is_last_feb {
                end_day = 30;
            }
            if start_is_last_feb {
                start_day = 30;
            }
            if end_day == 31 && start_day >= 30 {
                end_day = 30;
            }
            if start_day == 31 {
                start_day = 30;
            }
        }

        let days = (end_year - start_year) * 360 + (end_month - start_month) * 30
            + (end_day - start_day);

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(
            days as i64,
        )))
    }
}

fn is_last_day_of_month(date: &chrono::NaiveDate) -> bool {
    use chrono::Datelike;
    let next_day = *date + chrono::Duration::days(1);
    next_day.month() != date.month()
}

/// YEARFRAC(start_date, end_date, [basis]) - Returns the year fraction between two dates
#[derive(Debug)]
pub struct YearfracFn;

impl Function for YearfracFn {
    func_caps!(PURE);

    fn name(&self) -> &'static str {
        "YEARFRAC"
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
            vec![
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
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let start_serial = coerce_to_serial(&args[0])?;
        let end_serial = coerce_to_serial(&args[1])?;
        let basis = if args.len() > 2 {
            let v = args[2].value()?.into_literal();
            match v {
                LiteralValue::Number(n) => n as i32,
                LiteralValue::Int(i) => i as i32,
                _ => 0,
            }
        } else {
            0
        };

        if basis < 0 || basis > 4 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        let (start, end, swapped) = if start_serial <= end_serial {
            (start_serial, end_serial, false)
        } else {
            (end_serial, start_serial, true)
        };

        let start_date = serial_to_date(start)?;
        let end_date = serial_to_date(end)?;

        let result = match basis {
            0 => {
                // US (NASD) 30/360
                yearfrac_30_360_us(&start_date, &end_date)
            }
            1 => {
                // Actual/actual
                yearfrac_actual_actual(&start_date, &end_date)
            }
            2 => {
                // Actual/360
                let days = (end - start) as f64;
                days / 360.0
            }
            3 => {
                // Actual/365
                let days = (end - start) as f64;
                days / 365.0
            }
            4 => {
                // European 30/360
                yearfrac_30_360_eu(&start_date, &end_date)
            }
            _ => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            ))),
        };

        let final_result = if swapped { -result } else { result };
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            final_result.abs(),
        )))
    }
}

fn yearfrac_30_360_us(start: &chrono::NaiveDate, end: &chrono::NaiveDate) -> f64 {
    use chrono::Datelike;
    let mut sd = start.day() as i32;
    let mut sm = start.month() as i32;
    let sy = start.year();

    let mut ed = end.day() as i32;
    let em = end.month() as i32;
    let ey = end.year();

    // Adjust for last day of February
    let start_is_last_feb = sm == 2 && is_last_day_of_month(start);
    let end_is_last_feb = em == 2 && is_last_day_of_month(end);

    if start_is_last_feb && end_is_last_feb {
        ed = 30;
    }
    if start_is_last_feb {
        sd = 30;
    }
    if ed == 31 && sd >= 30 {
        ed = 30;
    }
    if sd == 31 {
        sd = 30;
    }

    let days = (ey - sy) * 360 + (em - sm) * 30 + (ed - sd);
    days as f64 / 360.0
}

fn yearfrac_30_360_eu(start: &chrono::NaiveDate, end: &chrono::NaiveDate) -> f64 {
    use chrono::Datelike;
    let mut sd = start.day() as i32;
    let sm = start.month() as i32;
    let sy = start.year();

    let mut ed = end.day() as i32;
    let em = end.month() as i32;
    let ey = end.year();

    if sd == 31 {
        sd = 30;
    }
    if ed == 31 {
        ed = 30;
    }

    let days = (ey - sy) * 360 + (em - sm) * 30 + (ed - sd);
    days as f64 / 360.0
}

fn yearfrac_actual_actual(start: &chrono::NaiveDate, end: &chrono::NaiveDate) -> f64 {
    use chrono::Datelike;
    let days = (*end - *start).num_days() as f64;

    // Determine if we span a leap year
    let sy = start.year();
    let ey = end.year();

    if sy == ey {
        let year_days = if is_leap_year(sy) { 366.0 } else { 365.0 };
        days / year_days
    } else {
        // Weighted average of years
        let mut total = 0.0;
        for y in sy..=ey {
            let year_start = if y == sy {
                *start
            } else {
                chrono::NaiveDate::from_ymd_opt(y, 1, 1).unwrap()
            };
            let year_end = if y == ey {
                *end
            } else {
                chrono::NaiveDate::from_ymd_opt(y + 1, 1, 1).unwrap()
            };
            let year_days = if is_leap_year(y) { 366.0 } else { 365.0 };
            let portion = (year_end - year_start).num_days() as f64 / year_days;
            total += portion;
        }
        total
    }
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

/// ISOWEEKNUM(date) - Returns the ISO week number of the year for a given date
#[derive(Debug)]
pub struct IsoWeeknumFn;

impl Function for IsoWeeknumFn {
    func_caps!(PURE);

    fn name(&self) -> &'static str {
        "ISOWEEKNUM"
    }

    fn min_args(&self) -> usize {
        1
    }

    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static ONE: LazyLock<Vec<ArgSchema>> =
            LazyLock::new(|| vec![ArgSchema::number_lenient_scalar()]);
        &ONE[..]
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let serial = coerce_to_serial(&args[0])?;
        let date = serial_to_date(serial)?;
        let iso_week = date.iso_week().week();
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(
            iso_week as i64,
        )))
    }
}

pub fn register_builtins() {
    use std::sync::Arc;
    crate::function_registry::register_function(Arc::new(YearFn));
    crate::function_registry::register_function(Arc::new(MonthFn));
    crate::function_registry::register_function(Arc::new(DayFn));
    crate::function_registry::register_function(Arc::new(HourFn));
    crate::function_registry::register_function(Arc::new(MinuteFn));
    crate::function_registry::register_function(Arc::new(SecondFn));
    crate::function_registry::register_function(Arc::new(DaysFn));
    crate::function_registry::register_function(Arc::new(Days360Fn));
    crate::function_registry::register_function(Arc::new(YearfracFn));
    crate::function_registry::register_function(Arc::new(IsoWeeknumFn));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use formualizer_parse::parser::{ASTNode, ASTNodeType};
    use std::sync::Arc;

    fn lit(v: LiteralValue) -> ASTNode {
        ASTNode::new(ASTNodeType::Literal(v), None)
    }

    #[test]
    fn test_year_month_day() {
        let wb = TestWorkbook::new()
            .with_function(Arc::new(YearFn))
            .with_function(Arc::new(MonthFn))
            .with_function(Arc::new(DayFn));
        let ctx = wb.interpreter();

        // Test with a known date serial number
        // Serial 44927 = 2023-01-01
        let serial = lit(LiteralValue::Number(44927.0));

        let year_fn = ctx.context.get_function("", "YEAR").unwrap();
        let result = year_fn
            .dispatch(
                &[ArgumentHandle::new(&serial, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(result, LiteralValue::Int(2023));

        let month_fn = ctx.context.get_function("", "MONTH").unwrap();
        let result = month_fn
            .dispatch(
                &[ArgumentHandle::new(&serial, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(result, LiteralValue::Int(1));

        let day_fn = ctx.context.get_function("", "DAY").unwrap();
        let result = day_fn
            .dispatch(
                &[ArgumentHandle::new(&serial, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(result, LiteralValue::Int(1));
    }

    #[test]
    fn test_hour_minute_second() {
        let wb = TestWorkbook::new()
            .with_function(Arc::new(HourFn))
            .with_function(Arc::new(MinuteFn))
            .with_function(Arc::new(SecondFn));
        let ctx = wb.interpreter();

        // Test with noon (0.5 = 12:00:00)
        let serial = lit(LiteralValue::Number(0.5));

        let hour_fn = ctx.context.get_function("", "HOUR").unwrap();
        let result = hour_fn
            .dispatch(
                &[ArgumentHandle::new(&serial, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(result, LiteralValue::Int(12));

        let minute_fn = ctx.context.get_function("", "MINUTE").unwrap();
        let result = minute_fn
            .dispatch(
                &[ArgumentHandle::new(&serial, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(result, LiteralValue::Int(0));

        let second_fn = ctx.context.get_function("", "SECOND").unwrap();
        let result = second_fn
            .dispatch(
                &[ArgumentHandle::new(&serial, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(result, LiteralValue::Int(0));

        // Test with 15:30:45 = 15.5/24 + 0.75/24/60 = 0.6463541667
        let time_serial = lit(LiteralValue::Number(0.6463541667));

        let hour_result = hour_fn
            .dispatch(
                &[ArgumentHandle::new(&time_serial, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(hour_result, LiteralValue::Int(15));

        let minute_result = minute_fn
            .dispatch(
                &[ArgumentHandle::new(&time_serial, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(minute_result, LiteralValue::Int(30));

        let second_result = second_fn
            .dispatch(
                &[ArgumentHandle::new(&time_serial, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(second_result, LiteralValue::Int(45));
    }
}
