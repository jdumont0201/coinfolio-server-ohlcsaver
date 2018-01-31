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
use std::sync::{Arc, Mutex};
use std::rc::Rc;
use std::cell::Cell;
use futures::{Future, Stream};
use std::collections::HashMap;
use std::fs::File;
use ws::{connect, Handler, Sender, Handshake, Result, Message, CloseCode, Response};
use std::thread;
use chrono::prelude::*;
use chrono::{DateTime, TimeZone, NaiveDateTime, Utc};
use rand::Rng;
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
fn getIdFromRow(val:String) -> String{
    println!("val {}",val);
    let idstr:Vec<&str>=val.split("id\":").collect();
    let idstrright=idstr[1];
    println!("val {}",idstrright);
    let idstrr:Vec<&str>=idstrright.split("}]").collect();
    let id=idstrr[0];

    id.to_string()
}
struct Pair {
    name: String,
    broker: String,
}

fn loadAndSaveOHLC(bb: &str, pp: &str) {
    let client = reqwest::Client::new();
    let mut lastTs = 0;
    let uri = format!("https://api.binance.com/api/v1/klines?symbol={}&interval={}&limit=2", pp.to_string(), "1m");
    let mut result: Vec<StringGenericOHLC> = Vec::new();
    if (bb == "bin") {
        if let Ok(mut res) = client.get(&uri).send() {
            println!("[{}] [GET_BRO] {}_ohlc ", pp.to_string(), res.status());
            let restext = res.text().unwrap();
            let res1 = &restext[2..restext.len() - 2];
            let resspl: Vec<&str> = res1.split("],[").collect();
            for row in resspl {
                if row.len() > 1 {
                    let res21: &str = &row[0..row.len()];
                    //println!("  {}", res21);
                    let r: Vec<&str> = res21.split(",").collect();
                    let oo = r[1];
                    let o = oo[1..oo.len() - 1].to_string();
                    let h = r[2][1..r[2].len() - 1].to_string();
                    let l = r[3][1..r[3].len() - 1].to_string();
                    let c = r[4][1..r[4].len() - 1].to_string();
                    let v = r[5][1..r[5].len() - 1].to_string();
                    let tss=parsei64(&r[0].to_string());
                    let ohlc: StringGenericOHLC = StringGenericOHLC {
                        ts: tss,
                        o: o,
                        h: h,
                        l: l,
                        c: c,
                        v: v,
                    };

                    if ohlc.ts != lastTs {
                        lastTs = ohlc.ts;

                        let json = ohlc.to_json(&pp.to_string());
                        result.push(ohlc);

                        let uri = format!("http://0.0.0.0:3000/{}_ohlc_1m", bb.to_string());
                        let tsss = chrono::Utc.timestamp(tss / 1000, 0).format("%Y-%m-%d %H:%M:%S");
                        let uriexists = format!("http://0.0.0.0:3000/{}_ohlc_1m?pair=eq.{}&ts=eq.'{}'", bb.to_string(),pp.to_string(),tsss);
                        if let Ok(mut res) = reqwest::get(&uriexists) {
                            let val:String=res.text().unwrap();
                            //println!("[{}] [GET_DA] {}_ohlc_1m existing? {} res={} len={}", pp.to_string(), bb.to_string(), res.status(), val, val.len());
                            if val.len()>2 {
                                println!("patch");
                                let id=getIdFromRow(val);
                                let uripatch = format!("http://0.0.0.0:3000/{}_ohlc_1m?id=eq.{}", bb.to_string(),id);
                                if let Ok(mut res) = client.patch(&uripatch).body(json).send() {
                              //      println!("[{}] [PATCH] {}_ohlc_1m {} res={} patchurl{}", pp.to_string(), bb.to_string(), res.status(), res.text().unwrap(),patchurl);
                                    let st=res.status();
                                    if st == hyper::StatusCode::NoContent {// ok
                                  //      println!("[{}] [PATCH] {}_ohlc_1m {} {}", pp.to_string(), bb.to_string(), res.status(), res.text().unwrap());
                                    } else{
                                    //    println!("[{}] [POST] {}_ohlc_1m {} {}", pp.to_string(), bb.to_string(), res.status(), res.text().unwrap());
                                    }
                                }else{
                                }

                            }else{
                                println!("post");
                                if let Ok(mut res) = client.post(&uri).body(json).send() {
                                    let st=res.status();
                                    if st == hyper::StatusCode::Conflict {//existing
                                //        println!("[{}] [POST] {}_ohlc_1m {} {}", pp.to_string(), bb.to_string(), res.status(), res.text().unwrap());
                                    }else if st==hyper::StatusCode::Created {//created
                              //          println!("[{}] [POST] {}_ohlc_1m {} {}", pp.to_string(), bb.to_string(), res.status(), res.text().unwrap());
                                    } else{
                            //            println!("[{}] [POST] {}_ohlc_1m {} {}", pp.to_string(), bb.to_string(), res.status(), res.text().unwrap());
                                    }
                            }
                        }

                        } else {
                            println!("[{}] [POST] nok uri", pp.to_string());
                        }
                    }
                } else {
                    println!("  err row {}", row);
                }
            }
        } else {
            println!("[{}] [GET] nok uri {}", pp, uri);
        }
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
                let delay = rand::thread_rng().gen_range(0, 5);
                thread::sleep(std::time::Duration::new(delay, 0));
                loadAndSaveOHLC(&bb,&pp);
            }));
            loop {
                sched.tick();
                std::thread::sleep(std::time::Duration::from_millis(500));
            }
        }));
    }
    for child in children {
        let _ = child.join();
    }
}