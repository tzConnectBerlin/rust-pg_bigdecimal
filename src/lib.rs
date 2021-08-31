use bytes::{BufMut, BytesMut};
use postgres::types::{to_sql_checked, FromSql, IsNull, ToSql, Type};
use std::convert::TryInto;

use bigdecimal::BigDecimal;
use num::{BigInt, BigUint, Integer};

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone)]
pub struct PgNumeric {
    pub n: Option<BigDecimal>,
}

impl PgNumeric {
    pub fn is_nan(&self) -> bool {
        self.n.is_none()
    }
}

use byteorder::{BigEndian, ReadBytesExt};
use std::io::Cursor;

impl<'a> FromSql<'a> for PgNumeric {
    fn from_sql(
        _: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + 'static + Sync + Send>> {
        let mut rdr = Cursor::new(raw);

        let n_digits = rdr.read_u16::<BigEndian>()?;
        let weight = rdr.read_i16::<BigEndian>()?;
        let sign = match rdr.read_u16::<BigEndian>()? {
            0x4000 => num::bigint::Sign::Minus,
            0x0000 => num::bigint::Sign::Plus,
            0xC000 => return Ok(Self { n: None }),
            _ => return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "").into()),
        };
        let scale = rdr.read_u16::<BigEndian>()?;

        let mut biguint = BigUint::from(0u32);
        for n in (0..n_digits).rev() {
            let digit = rdr.read_u16::<BigEndian>()?;
            biguint += BigUint::from(digit) * BigUint::from(10_000u32).pow(n as u32);
        }

        // First digit in unsigned now has factor 10_000^(digits.len() - 1),
        // but should have 10_000^weight
        //
        // Credits: this logic has been copied from rust Diesel's related code
        // that provides the same translation from Postgres numeric into their
        // related rust type.
        let correction_exp = 4 * (i64::from(weight) - i64::from(n_digits) + 1);
        let res = BigDecimal::new(BigInt::from_biguint(sign, biguint), -correction_exp)
            .with_scale(i64::from(scale));

        Ok(Self { n: Some(res) })
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::NUMERIC)
    }
}

impl ToSql for PgNumeric {
    fn to_sql(
        &self,
        _: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + 'static + Sync + Send>> {
        fn write_header(out: &mut BytesMut, n_digits: u16, weight: i16, sign: u16, scale: u16) {
            out.put_u16(n_digits);
            out.put_i16(weight);
            out.put_u16(sign);
            out.put_u16(scale);
        }
        fn write_body(out: &mut BytesMut, digits: &[i16]) {
            // write the body
            for digit in digits {
                out.put_i16(*digit);
            }
        }
        fn write_nan(out: &mut BytesMut) {
            // 8 bytes for the header (4 * 2byte numbers)
            out.reserve(8);
            write_header(out, 0, 0, 0xC000, 0);
            // no body for nan
        }

        match &self.n {
            None => {
                write_nan(out);
                Ok(IsNull::No)
            }
            Some(n) => {
                let (bigint, exponent) = n.as_bigint_and_exponent();
                let (sign, biguint) = bigint.into_parts();
                let neg = sign == num::bigint::Sign::Minus;
                let scale: i16 = exponent.try_into()?;

                let (integer, decimal) = biguint.div_rem(&BigUint::from(10u32).pow(scale as u32));
                let integer_digits: Vec<i16> = base10000(integer)?;
                let mut weight = integer_digits.len().try_into().map(|len: i16| len - 1)?;

                // must shift decimal part to align the decimal point between
                // two 10000 based digits.
                // note: shifted modulo by 1
                //       (resulting in 1..4 instead of 0..3 ranges)
                let decimal =
                    decimal * BigUint::from(10_u32).pow((4 - ((scale - 1) % 4 + 1)) as u32);
                let decimal_digits: Vec<i16> = base10000(decimal)?;

                let have_decimals_weight: i16 = decimal_digits.len().try_into()?;
                // the /4 is shifted by -1 to shift increments to
                // <multiples of 4 + 1>
                let want_decimals_weight = 1 + (scale - 1) / 4;
                let correction_weight = want_decimals_weight - have_decimals_weight;
                let mut decimal_zeroes_prefix: Vec<i16> = vec![];
                if integer_digits.is_empty() {
                    // if we have no integer part, can simply set weight to -
                    weight -= correction_weight;
                } else {
                    // if we do have an integer part, cannot save space.
                    //  we'll have to prefix the decimal part with 0 digits
                    decimal_zeroes_prefix = std::iter::repeat(0_i16)
                        .take(correction_weight.try_into()?)
                        .collect();
                }

                let mut digits: Vec<i16> = vec![];
                digits.extend(integer_digits);
                digits.extend(decimal_zeroes_prefix);
                digits.extend(decimal_digits);
                strip_trailing_zeroes(&mut digits);
                let n_digits = digits.len();

                // 8 bytes for the header (4 * 2byte numbers)
                // + 2 bytes per digit
                out.reserve(8 + n_digits * 2);

                write_header(
                    out,
                    n_digits.try_into()?,
                    weight,
                    if neg { 0x4000 } else { 0x0000 },
                    scale.try_into()?,
                );

                write_body(out, &digits);

                Ok(IsNull::No)
            }
        }
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::NUMERIC)
    }

    to_sql_checked!();
}

fn base10000(
    mut n: BigUint,
) -> Result<Vec<i16>, Box<dyn std::error::Error + 'static + Sync + Send>> {
    let mut res: Vec<i16> = vec![];

    while n != BigUint::from(0_u32) {
        let (remainder, digit) = n.div_rem(&BigUint::from(10_000u32));
        res.push(digit.try_into()?);
        n = remainder;
    }

    res.reverse();
    Ok(res)
}

fn strip_trailing_zeroes(digits: &mut Vec<i16>) {
    let mut truncate_at = 0;
    for (i, d) in digits.iter().enumerate().rev() {
        if *d != 0 {
            truncate_at = i + 1;
            break;
        }
    }
    digits.truncate(truncate_at);
}

#[test]
fn strip_trailing_zeroes_tests() {
    struct TestCase {
        inp: Vec<i16>,
        exp: Vec<i16>,
    }
    let test_cases: Vec<TestCase> = vec![
        TestCase {
            inp: vec![],
            exp: vec![],
        },
        TestCase {
            inp: vec![10, 5, 105],
            exp: vec![10, 5, 105],
        },
        TestCase {
            inp: vec![10, 5, 105, 0, 0, 0],
            exp: vec![10, 5, 105],
        },
        TestCase {
            inp: vec![0, 10, 0, 0, 5, 0, 105, 0, 0, 0],
            exp: vec![0, 10, 0, 0, 5, 0, 105],
        },
        TestCase {
            inp: vec![0],
            exp: vec![],
        },
    ];

    for tc in test_cases {
        let mut got = tc.inp.clone();
        strip_trailing_zeroes(&mut got);
        assert_eq!(tc.exp, got);
    }
}

#[test]
fn base10000_tests() {
    struct TestCase {
        inp: BigUint,
        exp: Vec<i16>,
    }
    let test_cases: Vec<TestCase> = vec![
        TestCase {
            inp: BigUint::parse_bytes(b"0", 10).unwrap(),
            exp: vec![],
        },
        TestCase {
            inp: BigUint::parse_bytes(b"1", 10).unwrap(),
            exp: vec![1],
        },
        TestCase {
            inp: BigUint::parse_bytes(b"10", 10).unwrap(),
            exp: vec![10],
        },
        TestCase {
            inp: BigUint::parse_bytes(b"100", 10).unwrap(),
            exp: vec![100],
        },
        TestCase {
            inp: BigUint::parse_bytes(b"1000", 10).unwrap(),
            exp: vec![1000],
        },
        TestCase {
            inp: BigUint::parse_bytes(b"9999", 10).unwrap(),
            exp: vec![9999],
        },
        TestCase {
            inp: BigUint::parse_bytes(b"10000", 10).unwrap(),
            exp: vec![1, 0],
        },
        TestCase {
            inp: BigUint::parse_bytes(b"100000000", 10).unwrap(),
            exp: vec![1, 0, 0],
        },
        TestCase {
            inp: BigUint::parse_bytes(b"900087000", 10).unwrap(),
            exp: vec![9, 8, 7000],
        },
    ];
    for tc in test_cases {
        let got = base10000(tc.inp);
        assert_eq!(tc.exp, got.unwrap());
    }
}

#[test]
fn integration_tests() {
    use postgres::{Client, NoTls};
    use std::str::FromStr;

    let mut dbconn = Client::connect(
        "host=localhost port=15432 user=test password=test dbname=test",
        NoTls,
    )
    .unwrap();

    dbconn
        .execute("CREATE TABLE IF NOT EXISTS foobar (n numeric)", &[])
        .unwrap();

    let mut test_for_pgnumeric = |pgnumeric| {
        dbconn.execute("DELETE FROM foobar;", &[]).unwrap();
        dbconn
            .execute("INSERT INTO foobar VALUES ($1)", &[&pgnumeric])
            .unwrap();

        let got: Option<PgNumeric> = dbconn
            .query_one("SELECT n FROM foobar", &[])
            .unwrap()
            .get(0);
        assert_eq!(pgnumeric, got.unwrap());
    };

    let tests = &[
        "10",
        "100",
        "1000",
        "10000",
        "10100",
        "30109",
        "0.1",
        "0.01",
        "0.001",
        "0.0001",
        "0.00001",
        "0.0000001",
        "1.1",
        "1.001",
        "1.00001",
        "3.14159265",
        "98756756756756756756756757657657656756756756756757656745644534534535435434567567656756757658787687676855674456345345364564.5675675675765765765765765756",
"204093200000000000000000000000000000000",
        "nan"
    ];
    for n in tests {
        let n = match n {
            &"nan" => PgNumeric { n: None },
            _ => PgNumeric {
                n: Some(BigDecimal::from_str(n).unwrap()),
            },
        };

        test_for_pgnumeric(n);
    }

    for n in tests {
        if n == &"nan" {
            continue;
        }

        let n = PgNumeric {
            n: Some(BigDecimal::from_str(n).unwrap() * BigDecimal::from(-1)),
        };
        test_for_pgnumeric(n);
    }
}
