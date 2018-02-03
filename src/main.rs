extern crate ws;
extern crate serde;
extern crate serde_json;
extern crate json;
extern crate clap;
extern crate ansi_term;
extern crate futures;
extern crate hyper;
extern crate tokio_core;
extern crate reqwest;
extern crate job_scheduler;
#[macro_use]
extern crate serde_derive;
extern crate chrono;
extern crate rand;


use std::collections::HashMap;
use std::fs::File;

use std::thread;
use chrono::prelude::*;
use chrono::{DateTime, TimeZone, NaiveDateTime, Utc};
use rand::Rng;

static DB_ADDRESS:&str="http://0.0.0.0:3000";

fn parsei64(i: &String) -> i64 {
    i.parse::<i64>().unwrap()
}

fn parsef64(i: &String) -> f64 {
    i.parse::<f64>().unwrap()
}

fn concat(a: &str, b: &str) -> String {
    let mut owned_str: String = "".to_owned();
    owned_str.push_str(a);
    owned_str.push_str(b);
    owned_str
}

pub struct StringGenericOHLC {
    ts: i64,
    o: String,
    h: String,
    c: String,
    l: String,
    v: String,
}

impl StringGenericOHLC {
    fn to_json(&self, pair: &str) -> String {
        let ts = chrono::Utc.timestamp(self.ts / 1000, 0).format("%Y-%m-%d %H:%M:%S");
        let s = format!(r#"{{"ts" :"{}","pair"  :"{}","open"  :"{}","high"  :"{}","low":"{}","close":"{}","volume":"{}"}}"#, ts, pair, self.o, self.h, self.l, self.c, self.v);
        s
    }
    fn to_string(&self) -> String {
        let mut owned_str: String = "".to_owned();
        owned_str.push_str(&(self.ts.to_string()).to_owned());
        owned_str.push_str(",");
        owned_str.push_str(&(self.o.to_string()).to_owned());
        owned_str.push_str(",");
        owned_str.push_str(&(self.h.to_string()).to_owned());
        owned_str.push_str(",");
        owned_str.push_str(&(self.l.to_string()).to_owned());
        owned_str.push_str(",");
        owned_str.push_str(&(self.c.to_string()).to_owned());
        owned_str.push_str(",");
        owned_str.push_str(&(self.v.to_string().to_owned()));
        owned_str.push_str("\n");
        owned_str
    }
}

fn getPairsFromArgs() -> Vec<Pair> {
    let args: Vec<String> = std::env::args().collect();
    let mut pairs: String;
    if let Ok(val) = std::env::var("PAIRS") {
        pairs = val.to_owned();
    } else {
        pairs = "bin:ETHUSDT".to_string()
    }
    println!("ENV PAIRS {}", pairs);
    let mut PAIRS: Vec<Pair> = Vec::new();
    let pairssp: Vec<&str> = pairs.split(",").collect();
    for p in &pairssp {
        let ppp: Vec<&str> = p.split(":").collect();
        if ppp.len() != 2 { println!("wrong format {}", p); }
        PAIRS.push(Pair { name: ppp[1].to_string(), broker: ppp[0].to_string() })
    }
    PAIRS
}

fn getIdFromRow(val: String) -> String {
    //println!("val {}", val);
    let idstr: Vec<&str> = val.split("id\":").collect();
    if idstr.len() == 0 {
        println!("err get_id_from_row {}", val);
        "".to_string()
    } else {
        let idstrright = idstr[1];
        //println!("val {}", idstrright);
        let idstrr: Vec<&str> = idstrright.split("}]").collect();
        let id = idstrr[0];

        id.to_string()
    }
}

struct Pair {
    name: String,
    broker: String,
}



mod Universal {
    use reqwest::Response;
    use StringGenericOHLC;
    use parsei64;
    use serde_json::from_str;

    fn getInterval(broker: &str, n: u8) -> String {
        let mut s: &str;
        if broker == "bin" {
            if n == 1 {
                s = "1m";
            } else {
                s = "";
            }
        } else if broker == "hit" {
            s = "M1";
        } else {
            s = "";
        }
        s.to_string()
    }

    pub fn get_url(task: &str, broker: &str, symbol: &str, interval: u8) -> String {
        let interstr = getInterval(broker, interval);
        let mut s: String;
        if task == "ohlc" {
            if broker == "bin" {
                s = format!("https://api.binance.com/api/v1/klines?symbol={}&interval={}&limit=2", symbol.to_string(), interstr);
            } else if broker == "hit" {
                s = format!("http://api.hitbtc.com/api/2/public/candles/{}?period={}&limit=2", symbol.to_string(), interstr);
            } else {
                s = "".to_string();
            }
        } else {
            s = "".to_string();
        }
        s
    }

    #[derive(Serialize, Deserialize)]
    struct hitbtc_ohlc {
        timestamp: String,
        open: String,
        close: String,
        min: String,
        max: String,
        volume: String,
        volumeQuote: String,
    }

    enum Value {
        Array(Vec<hitbtc_ohlc>),
    }

    pub fn get_ohlc_vec(task: &str, broker: String, request_res_text: String) -> Vec<StringGenericOHLC> {
        let mut result: Vec<StringGenericOHLC> = Vec::new();
        if broker == "bin" {
            let res1 = &request_res_text[2..request_res_text.len() - 2];
            let resspl: Vec<&str> = res1.split("],[").collect();
            for row in resspl {
                if row.len() > 1 {
                    let res21: &str = &row[0..row.len()];
                    let r: Vec<&str> = res21.split(",").collect();
                    let oo = r[1];
                    let o = oo[1..oo.len() - 1].to_string();
                    let h = r[2][1..r[2].len() - 1].to_string();
                    let l = r[3][1..r[3].len() - 1].to_string();
                    let c = r[4][1..r[4].len() - 1].to_string();
                    let v = r[5][1..r[5].len() - 1].to_string();
                    let tss: i64 = parsei64(&r[0].to_string());
                    let ohlc: StringGenericOHLC = StringGenericOHLC {
                        ts: tss,
                        o: o,
                        h: h,
                        l: l,
                        c: c,
                        v: v,
                    };
                    result.push(ohlc);
                }
            }
        } else if broker == "hit" {
            let bs: Vec<hitbtc_ohlc> = super::serde_json::from_str(&request_res_text).unwrap();

            //let bs: Vec<hitbtc_ohlc> = super::serde_json::from_str(&request_res_text);
            for b in bs {
                //println!("  serde {}",b.open);

                let tss: super::chrono::DateTime<super::chrono::Utc> = b.timestamp.parse::<super::chrono::DateTime<super::chrono::Utc>>().unwrap();
                //println!("  serde tss {:?}",tss);
                let tsi: i64 = tss.timestamp() * 1000;
                //println!("  serde tsi {}",tsi);
                let ohlc: StringGenericOHLC = StringGenericOHLC {
                    ts: tsi,
                    o: b.open,
                    h: b.max,
                    l: b.min,
                    c: b.close,
                    v: b.volume,
                };
                //println!("  serde ohlc {}",ohlc.o);
                result.push(ohlc);
            }
        }
        result
    }
}

fn save_ohlc(client: &reqwest::Client, broker: String, pair: String, ohlc: StringGenericOHLC) {
    let tss = ohlc.ts;
    let json = ohlc.to_json(&pair);

    let tsss = chrono::Utc.timestamp(tss / 1000, 0).format("%Y-%m-%d %H:%M:%S");
    let uriexists = format!("{}/{}_ohlc_1m?pair=eq.{}&ts=eq.'{}'",DB_ADDRESS, broker, pair, tsss);
    if let Ok(mut res) = reqwest::get(&uriexists) {
        let getres = match res.text() {
            Ok(val) => {
                if val.len() > 2 {
                    //println!("[{}/{}] patch",broker,pair);
                    let id = getIdFromRow(val);
                    let uripatch = format!("{}/{}_ohlc_1m?id=eq.{}", DB_ADDRESS,broker, id);
                    if let Ok(mut res) = client.patch(&uripatch).body(json).send() {
                        //      println!("[{}] [PATCH] {}_ohlc_1m {} res={} patchurl{}", pp.to_string(), bb.to_string(), res.status(), res.text().unwrap(),patchurl);
                        let st = res.status();
                        if st == hyper::StatusCode::NoContent {// ok
                            //      println!("[{}] [PATCH] {}_ohlc_1m {} {}", pp.to_string(), bb.to_string(), res.status(), res.text().unwrap());
                        } else {
                            //    println!("[{}] [POST] {}_ohlc_1m {} {}", pp.to_string(), bb.to_string(), res.status(), res.text().unwrap());
                        }
                    } else {}
                } else {
                    let uri = format!("{}/{}_ohlc_1m",DB_ADDRESS, broker);
                    //println!("[{}] post {} {}",pair.to_string(),json);

                    if let Ok(mut res) = client.post(&uri).body(json).send() {
                        let st = res.status();
                        //println!("[{}] [POST] {}_ohlc_1m {} {}", pp.to_string(), bb.to_string(), res.status(), res.text().unwrap());
                        if st == hyper::StatusCode::Conflict {//existing
                            //        println!("[{}] [POST] {}_ohlc_1m {} {}", pp.to_string(), bb.to_string(), res.status(), res.text().unwrap());
                        } else if st == hyper::StatusCode::Created {//created
                            //          println!("[{}] [POST] {}_ohlc_1m {} {}", pp.to_string(), bb.to_string(), res.status(), res.text().unwrap());
                        } else {
                            //            println!("[{}] [POST] {}_ohlc_1m {} {}", pp.to_string(), bb.to_string(), res.status(), res.text().unwrap());
                        }
                    }
                }
            }
            Err(err) => {
                println!("[{}] [GET_DA] !!{}_ohlc_1m existing? {} ", pair, broker, err);
            }
        };
        //println!("[{}] [GET_DA] {}_ohlc_1m existing? {} res={} len={}", pp.to_string(), bb.to_string(), res.status(), val, val.len());
    } else {
        println!("[{}] [POST] nok uri", pair);
    }
}

fn loadAndSaveOHLC(broker: &str, pair: &str) {
    let client = reqwest::Client::new();
    let uri = Universal::get_url("ohlc", broker, pair, 1);
    if let Ok(mut res) = client.get(&uri).send() {
        println!("[{}/{}] [GET] {} ", broker.to_string(), pair.to_string(), res.status());
        let result = match res.text() {
            Ok(text) => {
                let ohlc_vec: Vec<StringGenericOHLC> = Universal::get_ohlc_vec("ohlc", broker.to_string(), text);
                for bar in ohlc_vec {
                    save_ohlc(&client, broker.to_string(), pair.to_string(), bar);
                }
            }
            Err(err) => {
                println!("[{}] [GET_BRO] ohlc ERR !!!  {}", pair.to_string(), err);
            }
        };
    }
}

mod CoinMarketCap {
    use reqwest;
    use chrono;
    use hyper;
    use chrono::prelude::*;
    use chrono::{DateTime, TimeZone, NaiveDateTime, Utc};
    #[derive(Serialize, Deserialize)]
   pub struct Data {
        id: String,
        name: String,
        symbol: String,
        rank: String,
        price_usd: String,
        price_btc: String,
        // 24h_volume_usd: String,
        market_cap_usd: String,
        available_supply: String,
        total_supply: String,
        max_supply: Option<String>,
        percent_change_1h: String,
        percent_change_24h: String,
        percent_change_7d: String,
        last_updated: String,
    }
    impl Data{
        fn to_json(&self) -> String {
            let tsi:i64=self.last_updated.parse::<i64>().unwrap();
            let ts = super::chrono::Utc.timestamp(tsi , 0).format("%Y-%m-%d %H:%M:%S");
            let maxsup;let s;
            match self.max_supply{
                Some(ref ma)=>{
                    maxsup=ma.to_string();
                    s = format!(r#"{{"ts" :"{}","symbol"  :"{}","marketcap"  :"{}","supply":"{}","maxsupply":"{}"}}"#, ts, self.symbol,self.market_cap_usd,self.total_supply,maxsup);
                },None=>{

                    s = format!(r#"{{"ts" :"{}","symbol"  :"{}","marketcap"  :"{}","supply":"{}"}}"#, ts, self.symbol,self.market_cap_usd,self.total_supply);
                }
            };

            s
        }
    }
    fn parse(text: &str) {}

    pub fn save_coinmarketcap(client: &reqwest::Client,  data: Data) {
        let tsi=data.last_updated.parse::<i64>().unwrap();
        let ts = chrono::Utc.timestamp(tsi , 0).format("%Y-%m-%d %H:%M:%S");
        let json = data.to_json();
        let uriexists = format!("{}/cmc_cap?symbol=eq.{}&ts=eq.'{}'",super::DB_ADDRESS, data.symbol,ts);
        println!("save {}",json);
        if let Ok(mut res) = reqwest::get(&uriexists) {
            //println!("get ok");
            let getres = match res.text() {
                Ok(val) => {
              //      println!("getres {}",val);
                    if val.len() > 2 { //already exists, do nothing

                    } else {
                        let uri = format!("{}/cmc_cap",super::DB_ADDRESS);
                //        println!("post {}",json);

                        if let Ok(mut res) = client.post(&uri).body(json).send() {

                            let st = res.status();
                  //          println!("post st {}",st);
                            //println!("[{}] [POST] {}_ohlc_1m {} {}", pp.to_string(), bb.to_string(), res.status(), res.text().unwrap());
                            if st == hyper::StatusCode::Conflict {//existing
                                //        println!("[{}] [POST] {}_ohlc_1m {} {}", pp.to_string(), bb.to_string(), res.status(), res.text().unwrap());
                            } else if st == hyper::StatusCode::Created {//created
                                //          println!("[{}] [POST] {}_ohlc_1m {} {}", pp.to_string(), bb.to_string(), res.status(), res.text().unwrap());
                            } else {
                                //            println!("[{}] [POST] {}_ohlc_1m {} {}", pp.to_string(), bb.to_string(), res.status(), res.text().unwrap());
                            }
                        }
                    }
                }
                Err(err) => {
                    println!("[GET_CMC] !!cmc_cap existing? {} ", err);
                }
            };
        } else {
            println!("[GET] nok uri {}",uriexists);
        }
    }
}


fn fetch_and_save_cmc() {
    println!(" -> CMC market cap");
    let client = reqwest::Client::new();
    let uri = "https://api.coinmarketcap.com/v1/ticker/";
    if let Ok(mut res) = client.get(uri).send() {
        println!("[GET] {} ", res.status());
        let result = match res.text() {
            Ok(text) => {
                let data: Vec<CoinMarketCap::Data> = serde_json::from_str(&text).unwrap();
                for d in data {
                    CoinMarketCap::save_coinmarketcap(&client,d);
                }
            }
            Err(err) => {
                println!(" [GET_CAP] cap ERR !!!  {}",  err);
            }
        };
    }
}


fn main() {
    println!("Coinamics Server OHLC saver");
    let mut children = vec![];
    let PAIRS = getPairsFromArgs();
    let nb = PAIRS.len();
    println!("Loading {} pairs", nb);
    println!("Starting pair threads");
    for p in PAIRS.iter() {
        println!("[{}/{}] Starting thread", p.broker, p.name);
        let pp = p.name.clone();
        let bb = p.broker.clone();
        children.push(thread::spawn(move || {
            let mut sched = job_scheduler::JobScheduler::new();
            let mut rng = rand::thread_rng();

            sched.add(job_scheduler::Job::new("10 * * * * *".parse().unwrap(), || {
                let delay = rand::thread_rng().gen_range(0, 10);
                thread::sleep(std::time::Duration::new(delay, 0));
            //    loadAndSaveOHLC(&bb, &pp);
            }));
            loop {
                sched.tick();
                std::thread::sleep(std::time::Duration::from_millis(500));
            }
        }));
    }

    children.push(thread::spawn(move || {
        println!("Starting CMC  threads");
        let mut sched = job_scheduler::JobScheduler::new();
        sched.add(job_scheduler::Job::new("30 1,6,11,16,21,26,31,36,41,46,51,56 * * * *".parse().unwrap(), || {
            let delay = rand::thread_rng().gen_range(0, 10);
            thread::sleep(std::time::Duration::new(delay, 0));
            fetch_and_save_cmc();
        }));
        loop {
            sched.tick();
            std::thread::sleep(std::time::Duration::from_millis(1000));
        }
    }));
    for child in children {
        let _ = child.join();
    }
}