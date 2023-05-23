#![feature(associated_type_defaults)]

use std::fs::File;
use chrono;
use std::io::{BufRead, BufReader, Error, ErrorKind};
use std::sync::Arc;

use yahoo_finance_api as yahoo;
use async_trait::async_trait;
use std::time::{Duration, UNIX_EPOCH};
use time::{OffsetDateTime};
use tokio::{time as ttime};
use futures::future::join_all;

const SYMBOL_PATH: &str = "sp500.dec.2022.csv";

struct PriceDifference {}

struct MinPrice {}

struct MaxPrice {}

struct WindowedSMA {
    window_size: i32,
}

///
/// A trait to provide a common interface for all signal calculations.
///
#[async_trait]
trait AsyncStockSignal {
    ///
    /// The signal's data type.
    ///
    type SignalType;

    ///
    /// Calculate the signal on the provided series.
    ///
    /// # Returns
    ///
    /// The signal (using the provided type) or `None` on error/invalid data.
    ///
    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType>;
}

#[async_trait]
impl AsyncStockSignal for PriceDifference {
    type SignalType = (f64, f64);

    ///
    /// Calculates the absolute and relative difference between the beginning and ending of an f64 series. The relative difference is relative to the beginning.
    ///
    /// # Returns
    ///
    /// A tuple `(absolute, relative)` difference.
    ///
    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType> {
        if !series.is_empty() {
            // unwrap is safe here even if first == last
            let (first, last) = (series.first().unwrap(), series.last().unwrap());
            let abs_diff = last - first;
            let first = if *first == 0.0 { 1.0 } else { *first };
            let rel_diff = abs_diff / first;
            Some((abs_diff, rel_diff))
        } else {
            None
        }
    }
}

#[async_trait]
impl AsyncStockSignal for MinPrice {
    type SignalType = f64;

    ///
    /// Find the minimum in a series of f64
    ///
    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType> {
        if series.is_empty() {
            None
        } else {
            Some(series.iter().fold(f64::MAX, |acc, q| acc.min(*q)))
        }
    }
}

#[async_trait]
impl AsyncStockSignal for MaxPrice {
    type SignalType = f64;

    ///
    /// Find the maximum in a series of f64
    ///
    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType> {
        if series.is_empty() {
            None
        } else {
            Some(series.iter().fold(f64::MIN, |acc, q| acc.max(*q)))
        }
    }
}

#[async_trait]
impl AsyncStockSignal for WindowedSMA {
    type SignalType = Vec<f64>;

    ///
    /// Window function to create a simple moving average
    ///
    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType> {
        if !series.is_empty() && self.window_size > 1 {
            Some(
                series
                    .windows(self.window_size as usize)
                    .map(|w| w.iter().sum::<f64>() / w.len() as f64)
                    .collect(),
            )
        } else {
            None
        }
    }
}

///
/// Retrieve data from a data source and extract the closing prices. Errors during download are mapped onto io::Errors as InvalidData.
///
async fn fetch_closing_data(
    symbol: &str,
    beginning: &OffsetDateTime,
    end: &OffsetDateTime,
) -> std::io::Result<Vec<f64>> {
    let provider = yahoo::YahooConnector::new();
    let response = provider
        .get_quote_history(symbol, *beginning, *end)
        .await
        .map_err(|e| {
            println!("{}", e.to_string());
            return Error::from(ErrorKind::Other);
        })?;
    let mut quotes = response
        .quotes()
        .map_err(|_| Error::from(ErrorKind::InvalidData))?;
    if !quotes.is_empty() {
        quotes.sort_by_cached_key(|k| k.timestamp);
        Ok(quotes.iter().map(|q| q.adjclose as f64).collect())
    } else {
        Ok(vec![])
    }
}

async fn process_symbol_data(symbol: &str,
                             from: &OffsetDateTime,
                             end: &OffsetDateTime) {
    let closes = fetch_closing_data(&symbol, &from, &end).await.unwrap();
    if !closes.is_empty() {
        let max_price = MaxPrice {};
        let min_price = MinPrice {};
        let price_difference = PriceDifference {};
        let windowed_sma = WindowedSMA { window_size: 30 };

        // min/max of the period. unwrap() because those are Option types
        let (period_max, period_min, price_diff, sma)
            = tokio::join!(
                        max_price.calculate(&closes),
                        min_price.calculate(&closes),
                        price_difference.calculate(&closes),
                        windowed_sma.calculate( &closes)
                    );
        let (_, pct_change) = price_diff.unwrap_or((0.0, 0.0));
        let last_price = *closes.last().unwrap_or(&0.0);

        // a simple way to output CSV data
        println!(
            "{},{},${:.2},{:.2}%,${:.2},${:.2},${:.2}",
            from.to_string(),
            symbol,
            last_price,
            pct_change * 100.0,
            period_min.unwrap(),
            period_max.unwrap(),
            sma.unwrap_or_default().last().unwrap_or(&0.0)
        );
    }
}

async fn read_data(file_path: &str) -> std::io::Result<Vec<String>> {
    let file = File::open(file_path).expect("Failed to open the file");
    let reader = BufReader::new(file);

    let data = reader.lines()
        .filter_map(Result::ok)
        .collect();

    Ok(data)
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let sp500_symbols = read_data(SYMBOL_PATH).await?;
    let total_symbols = sp500_symbols.len();
    let symbols = Arc::new(sp500_symbols);

    // a simple way to output a CSV header
    println!("period start,symbol,price,change %,min,max,30d avg");
    let mut tasks = vec![];
    for s in 0..total_symbols {
        let arc_symbols = symbols.clone();
        let symbol = (arc_symbols.as_ref())[s].clone();
        let handle = tokio::spawn(async move {
            let mut interval = ttime::interval(std::time::Duration::from_secs(30));
            loop {
                interval.tick().await;
                let elapsed_time = chrono::Utc::now() + chrono::Duration::seconds(30);
                let from = OffsetDateTime::from_unix_timestamp(elapsed_time.timestamp()).expect("Failed to convert to offset date time");
                let to = OffsetDateTime::from_unix_timestamp(chrono::Utc::now().timestamp()).expect("Failed to convert to offset date time");
                process_symbol_data(&symbol, &to, &from).await;
            }
        });
        tasks.push(handle);
    }
    join_all(tasks).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(non_snake_case)]

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_PriceDifference_calculate() {
        let signal = PriceDifference {};
        assert_eq!(signal.calculate(&[]).await, None);
        assert_eq!(signal.calculate(&[1.0]).await, Some((0.0, 0.0)));
        assert_eq!(signal.calculate(&[1.0, 0.0]).await, Some((-1.0, -1.0)));
        assert_eq!(
            signal.calculate(&[2.0, 3.0, 5.0, 6.0, 1.0, 2.0, 10.0]).await,
            Some((8.0, 4.0))
        );
        assert_eq!(
            signal.calculate(&[0.0, 3.0, 5.0, 6.0, 1.0, 2.0, 1.0]).await,
            Some((1.0, 1.0))
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_MinPrice_calculate() {
        let signal = MinPrice {};
        assert_eq!(signal.calculate(&[]).await, None);
        assert_eq!(signal.calculate(&[1.0]).await, Some(1.0));
        assert_eq!(signal.calculate(&[1.0, 0.0]).await, Some(0.0));
        assert_eq!(
            signal.calculate(&[2.0, 3.0, 5.0, 6.0, 1.0, 2.0, 10.0]).await,
            Some(1.0)
        );
        assert_eq!(
            signal.calculate(&[0.0, 3.0, 5.0, 6.0, 1.0, 2.0, 1.0]).await,
            Some(0.0)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_MaxPrice_calculate() {
        let signal = MaxPrice {};
        assert_eq!(signal.calculate(&[]).await, None);
        assert_eq!(signal.calculate(&[1.0]).await, Some(1.0));
        assert_eq!(signal.calculate(&[1.0, 0.0]).await, Some(1.0));
        assert_eq!(
            signal.calculate(&[2.0, 3.0, 5.0, 6.0, 1.0, 2.0, 10.0]).await,
            Some(10.0)
        );
        assert_eq!(
            signal.calculate(&[0.0, 3.0, 5.0, 6.0, 1.0, 2.0, 1.0]).await,
            Some(6.0)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_WindowedSMA_calculate() {
        let series = vec![2.0, 4.5, 5.3, 6.5, 4.7];

        let signal = WindowedSMA { window_size: 3 };
        assert_eq!(
            signal.calculate(&series).await,
            Some(vec![3.9333333333333336, 5.433333333333334, 5.5])
        );

        let signal = WindowedSMA { window_size: 5 };
        assert_eq!(signal.calculate(&series).await, Some(vec![4.6]));

        let signal = WindowedSMA { window_size: 10 };
        assert_eq!(signal.calculate(&series).await, Some(vec![]));
    }

    #[tokio::test]
    async fn test_reading_from_file() {
        let symbols = vec!["MMM", "AOS", "ABT", "ABBV", "ACN", "ATVI", "ADM", "ADBE", "ADP", "AAP"];

        let res = read_data("test.csv").await;
        assert_eq!(
            res.unwrap(),
            symbols
        );
    }
}
