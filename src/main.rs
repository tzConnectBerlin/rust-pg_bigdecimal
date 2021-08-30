use anyhow::{anyhow, Result};

use postgres::{Client, NoTls};

use bytes::{BufMut, BytesMut};
use postgres::types::{to_sql_checked, FromSql, IsNull, ToSql, Type};
use std::convert::TryInto;

use bigdecimal::BigDecimal;
use num::{BigInt, BigUint, Integer};
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
        let n = PgNumeric {
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
            let got: Option<PgNumeric> = row.get(1);
            println!("{:?}: {:?}", id, got);
            assert_eq!(n.n, got.unwrap().n);
        }
    }

    for n in tests {
        let t = 103;
        let n = PgNumeric {
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
            let got: Option<PgNumeric> = row.get(1);
            println!("{:?}: {:?}", id, got);
            assert_eq!(n.n, got.unwrap().n);
        }
    }
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone)]
pub struct PgNumeric {
    pub n: Option<BigDecimal>,
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

        let mut unsigned = BigUint::from(0u32);
        for n in (0..n_digits).rev() {
            let digit = rdr.read_i16::<BigEndian>()?;
            unsigned += BigUint::from(digit as u16) * BigUint::from(10_000u32).pow(n as u32);
        }

        // First digit in unsigned now has factor 10_000^(digits.len() - 1),
        // but should have 10_000^weight
        //
        // Credits: this logic has been copied from rust Diesel's related code
        // provides the same translation from Postgres numeric into their related
        // rust type.
        let correction_exp = 4 * (i64::from(weight) - i64::from(n_digits) + 1);
        let res = BigDecimal::new(BigInt::from_biguint(sign, unsigned), -correction_exp)
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
        fn base10000(mut n: BigUint) -> Vec<i16> {
            let mut res: Vec<i16> = vec![];

            while n != BigUint::from(0_u32) {
                let (remainder, digit) = n.div_rem(&BigUint::from(10_000u32));
                res.push(digit.try_into().unwrap());
                n = remainder;
            }

            res.reverse();
            res
        }

        let (bigint, exponent) = self.n.as_ref().unwrap().as_bigint_and_exponent();
        let (sign, biguint) = bigint.into_parts();
        let neg = sign == num::bigint::Sign::Minus;
        let scale: i16 = exponent.try_into().unwrap();

        let (integer, decimal) = biguint.div_rem(&BigUint::from(10u32).pow(scale as u32));
        let mut integer_digits: Vec<i16> = base10000(integer);
        let mut weight = integer_digits.len() as i16 - 1;

        // must shift decimal part to align the decimal point between 2 10000
        // based digits.
        // shifted modulo by 1 (resulting in (0..4] instead of [0..4) ranges)
        let decimal = decimal * BigUint::from(10_u32).pow((4 - ((scale - 1) % 4 + 1)) as u32);
        let decimal_digits = base10000(decimal);

        let have_decimals_weight = decimal_digits.len() as i16;
        // /4 shifted by -1 to shift increments to <multiples of 4 + 1>
        let want_decimals_weight = 1 + ((scale - 1) as i16) / 4;
        let correction_weight = want_decimals_weight - have_decimals_weight;
        if integer_digits.len() == 0 {
            // if we have no integer part, can simply set weight to -
            weight -= correction_weight;
        } else {
            // if we do have an integer part, cannot safe space. we'll have to
            // prefix the decimal with 0 digits
            //
            // Note: we append to the integer digits but it's effectively
            // creating a prefix for the decimal part
            integer_digits.extend(std::iter::repeat(0_i16).take(correction_weight as usize));
        }

        let mut digits: Vec<i16> = vec![];
        digits.extend(integer_digits);
        digits.extend(decimal_digits);

        let num_digits = digits.len();
        // 8 bytes for the header (4 * 2byte numbers), + 2 bytes per digit
        out.reserve(8 + num_digits * 2);

        // write the header
        out.put_u16(num_digits.try_into().unwrap());
        out.put_i16(weight);
        out.put_u16(if neg { 0x4000 } else { 0x0000 });
        out.put_u16(scale as u16);

        // write the body
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
