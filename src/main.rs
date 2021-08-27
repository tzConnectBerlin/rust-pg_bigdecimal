use anyhow::{anyhow, Result};

use postgres::{Client, NoTls};

use bytes::{BufMut, BytesMut};
use postgres::types::{to_sql_checked, FromSql, IsNull, ToSql, Type};
use std::convert::TryInto;

use bigdecimal::BigDecimal;
use num::BigInt;
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
    ];
    for n in tests {
        println!("\n----\ntesting: {}", n);
        let t = 103;
        let n = Numeric {
            n: BigDecimal::from_str(n).unwrap(),
        };

        dbconn
            .execute(
                "
INSERT INTO foobar (i, n)
VALUES ($1, $2)",
                &[&t, &n],
            )
            .unwrap();
    }
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone)]
pub struct Numeric {
    pub n: BigDecimal,
}

impl ToSql for Numeric {
    fn to_sql(
        &self,
        _: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + 'static + Sync + Send>> {
        let neg = false;

        let (bigint, exponent) = self.n.as_bigint_and_exponent();
        let scale: u16 = exponent.try_into().unwrap();

        // This doesn't work yet: bigint operations
        let mut digits: Vec<i16> = vec![];
        let mut decimal = bigint.clone() % BigInt::from(10).pow(scale as u32);
        println!("frac: {}", decimal);
        decimal *= BigInt::from(10).pow((4 - ((scale - 1) % 4 + 1)) as u32);
        println!("frac: {}", decimal);
        let zero = BigInt::from(0);
        while decimal != zero {
            digits.push((decimal.clone() % BigInt::from(10_000)).try_into().unwrap());
            decimal /= BigInt::from(10_000);
        }

        println!("scale: {}", scale);
        let mut integer = bigint.clone() / BigInt::from(10).pow(scale as u32);
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
                digits.push((integer.clone() % BigInt::from(10_000)).try_into().unwrap());
                integer /= BigInt::from(10_000);
                weight += 1;
            }
        }
        digits.reverse();
        println!("..{:?}", digits);

        // This works: string based operations
        //let mut digits: Vec<i16> = vec![];
        //for s in bigint.to_str_radix(10).as_bytes()[0..s.len() - (scale as usize)]
        //    .rchunks(4)
        //    .map(|buf| unsafe { std::str::from_utf8_unchecked(buf) })
        //    .rev()
        //    .collect::<Vec<&str>>()
        //{
        //    let digit = s.parse::<i16>().unwrap();
        //    digits.push(digit);
        //}
        //for s in bigint.to_str_radix(10).as_bytes()[s.len() - (scale as usize)..]
        //    .chunks(4)
        //    .map(|buf| unsafe { std::str::from_utf8_unchecked(buf) })
        //    .collect::<Vec<&str>>()
        //{
        //    let s = format!(
        //        "{}{}",
        //        s,
        //        std::iter::repeat('0').take(4 - s.len()).collect::<String>()
        //    );
        //    let digit = s.parse::<i16>().unwrap();
        //    digits.push(digit);
        //}

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
        out.put_u16(scale);
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
