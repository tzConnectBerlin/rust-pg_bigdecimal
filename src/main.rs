use anyhow::{anyhow, Result};

use postgres::{Client, NoTls};

use bytes::{BufMut, BytesMut};
use postgres::types::{to_sql_checked, FromSql, IsNull, ToSql, Type};
use std::convert::TryInto;

use bigdecimal::BigDecimal;
use num::{BigInt, BigUint};
use std::str::FromStr;

fn main() {
    let mut dbconn = connect(
        //        "host=192.168.8.129 dbname=test user=newby password=foobar port=5432",
        "host=0.0.0.0 user=postgres password=admin port=15432",
        false,
        None,
    )
    .unwrap();

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
    ];
    for n in tests {
        println!("\n----\ntesting: {}", n);
        let t = 103;
        let n = Numeric {
            n: Some(BigDecimal::from_str(n).unwrap()),
        };

        dbconn
            .execute(
                "
DELETE FROM foobar;
",
                &[],
            )
            .unwrap();

        dbconn
            .execute(
                "
INSERT INTO foobar (i, n)
VALUES ($1, $2)",
                &[&t, &n],
            )
            .unwrap();

        println!(">>>");

        for row in dbconn
            .query(
                "
SELECT id, n
FROM foobar
",
                &[],
            )
            .unwrap()
        {
            let id: Option<i32> = row.get(0);
            let got: Option<Numeric> = row.get(1);
            println!("{:?}: {:?}", id, got);
            assert_eq!(n.n, got.unwrap().n);
        }
    }

    for n in tests {
        let t = 103;
        let n = Numeric {
            n: Some(BigDecimal::from_str(n).unwrap() * BigDecimal::from(-1)),
        };
        println!("\n----\ntesting: {:?}", n.n);

        dbconn
            .execute(
                "
DELETE FROM foobar;
",
                &[],
            )
            .unwrap();

        dbconn
            .execute(
                "
INSERT INTO foobar (i, n)
VALUES ($1, $2)",
                &[&t, &n],
            )
            .unwrap();

        println!(">>>");

        for row in dbconn
            .query(
                "
SELECT id, n
FROM foobar
",
                &[],
            )
            .unwrap()
        {
            let id: Option<i32> = row.get(0);
            let got: Option<Numeric> = row.get(1);
            println!("{:?}: {:?}", id, got);
            assert_eq!(n.n, got.unwrap().n);
        }
    }
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone)]
pub struct Numeric {
    pub n: Option<BigDecimal>,
}

use byteorder::{BigEndian, ReadBytesExt};
use std::io::Cursor;

impl<'a> FromSql<'a> for Numeric {
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
        let mut scale = rdr.read_u16::<BigEndian>()? as i16;

        println!(
            "sign={:?}, scale={}, weight={}, n_digits={}",
            sign, scale, weight, n_digits
        );

        let w_offset = weight - (n_digits - 1) as i16;
        println!("w_offset: {}", w_offset);

        let mut unsigned = BigUint::from(0 as u32);
        for n in (0..n_digits).rev() {
            let digit = rdr.read_i16::<BigEndian>()?;
            println!("digit: {}", digit);
            //let mut w = BigUint::from(10u32).pow(n as u32);
            //if n < n_digits - scale {
            println!("n: {}", n);
            let w = BigUint::from(10_000u32).pow(n as u32);
            //}
            unsigned += BigUint::from(digit as u16) * w;
        }
        println!("unsigned: {}", unsigned);

        /*
        let mut bi = BigInt::from_biguint(sign, unsigned);
        //if weight < 0 {
        //    scale =
        //}
        let orig_scale = scale;
        if w_offset >= 0 {
            bi *= BigInt::from(10_000u32).pow(w_offset as u32);
        } else {
            //scale += 5 * (w_offset * -1);
        }
        let mut res = BigDecimal::new(bi, scale as i64); // .with_scale(orig_scale as i64);
                                                         if w_offset < 0 {
                                                             res = res / BigDecimal::new(BigInt::from(10_000u32).pow((w_offset * -1) as u32), 0);
                                                         }
                                                         */

        // First digit got factor 10_000^(digits.len() - 1), but should get 10_000^weight
        let correction_exp = 4 * (i64::from(weight) - i64::from(n_digits) + 1);
        let res = BigDecimal::new(BigInt::from_biguint(sign, unsigned), -correction_exp)
            .with_scale(i64::from(scale));

        Ok(Self { n: Some(res) })
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::NUMERIC)
    }
}

impl ToSql for Numeric {
    fn to_sql(
        &self,
        _: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + 'static + Sync + Send>> {
        let (bigint, exponent) = self.n.as_ref().unwrap().as_bigint_and_exponent();
        let (sign, biguint) = bigint.into_parts();
        let neg = sign == num::bigint::Sign::Minus;

        let scale: i16 = exponent.try_into().unwrap();

        // This doesn't work yet: bigint operations
        let mut digits: Vec<i16> = vec![];
        let mut decimal = biguint.clone() % BigUint::from(10u32).pow(scale as u32);
        println!("frac: {}", decimal);
        decimal *= BigUint::from(10_u32).pow((4 - ((scale - 1) % 4 + 1)) as u32);
        println!("frac: {}", decimal);
        let zero = BigUint::from(0_u32);
        while decimal != zero {
            digits.push(
                (decimal.clone() % BigUint::from(10_000u32))
                    .try_into()
                    .unwrap(),
            );
            decimal /= BigUint::from(10_000u32);
        }

        println!("scale: {}", scale);
        let mut integer = biguint.clone() / BigUint::from(10u32).pow(scale as u32);
        // scale/4 - digits.len()
        let mut weight: i16 = -1;

        let n = digits.len() as i16;
        let m = 1 + ((scale - 1) as i16) / 4;
        println!("{}, {}", n, m);
        if integer == zero {
            weight -= m - n;
        } else {
            digits.extend(std::iter::repeat(0 as i16).take((m - n) as usize));
            while integer != zero {
                digits.push(
                    (integer.clone() % BigUint::from(10_000u32))
                        .try_into()
                        .unwrap(),
                );
                integer /= BigUint::from(10_000u32);
                weight += 1;
            }
        }
        digits.reverse();
        println!("..{:?}", digits);

        println!(
            "neg={}, scale={}, weight={}, digits={:?}",
            neg, scale, weight, digits
        );

        let num_digits = digits.len();

        // Reserve bytes
        out.reserve(8 + num_digits * 2);

        // Number of groups
        out.put_u16(num_digits.try_into().unwrap());
        // Weight of first group
        out.put_i16(weight);
        // Sign
        out.put_u16(if neg { 0x4000 } else { 0x0000 });
        // DScale
        out.put_u16(scale as u16);
        // Now process the number
        for digit in digits[0..num_digits].iter() {
            out.put_i16(*digit);
        }

        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::NUMERIC)
    }

    to_sql_checked!();
}

fn connect(url: &str, ssl: bool, _ca_cert: Option<String>) -> Result<Client> {
    if ssl {
        Err(anyhow!(""))
        //    let mut builder = TlsConnector::builder();
        //    if let Some(ca_cert) = ca_cert {
        //        builder.add_root_certificate(Certificate::from_pem(&fs::read(ca_cert)?)?);
        //    }
        //    let connector = builder.build()?;
        //    let connector = MakeTlsConnector::new(connector);

        //    Ok(postgres::Client::connect(url, connector)?)
    } else {
        Ok(Client::connect(url, NoTls)?)
    }
}
