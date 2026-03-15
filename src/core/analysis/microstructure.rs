// Market microstructure analysis — tradeability scoring from order book data.

use crate::core::types::{OrderBook, Side};

/// Assessment of how tradeable a market currently is.
#[derive(Debug, Clone)]
pub struct TradeabilityAssessment {
    /// Spread as percentage of midpoint (lower = better)
    pub spread_pct: f64,
    /// USD depth at 5% slippage on the ask side
    pub ask_depth_usd: f64,
    /// USD depth at 5% slippage on the bid side
    pub bid_depth_usd: f64,
    /// Expected slippage for a $100 buy
    pub slippage_100_pct: f64,
    /// Number of price levels that would be crossed for $100
    pub levels_for_100: usize,
    /// Best ask price (entry price for buyers)
    pub best_ask: f64,
    /// Best bid price (exit price for sellers)
    pub best_bid: f64,
    /// Overall tradeability: "good", "marginal", or "untradeable"
    pub rating: TradeabilityRating,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeabilityRating {
    Good,
    Marginal,
    Untradeable,
}

impl std::fmt::Display for TradeabilityRating {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TradeabilityRating::Good => write!(f, "good"),
            TradeabilityRating::Marginal => write!(f, "marginal"),
            TradeabilityRating::Untradeable => write!(f, "untradeable"),
        }
    }
}

/// Analyze tradeability of a market from its order book.
/// Returns None if the book is empty (no bids or asks).
pub fn assess_tradeability(book: &OrderBook) -> Option<TradeabilityAssessment> {
    let best_ask = book.best_ask()?;
    let best_bid = book.best_bid()?;
    let spread_pct = book.spread_pct().unwrap_or(f64::MAX);

    let ask_depth_usd = book.ask_depth_within(0.05);
    let bid_depth_usd = book.bid_depth_within(0.05);

    let (slippage_100_pct, levels_for_100) =
        if let Some(fill) = book.simulate_fill_usd(Side::Buy, 100.0) {
            (fill.slippage_pct, fill.levels_crossed)
        } else {
            (f64::MAX, 0)
        };

    let rating = rate_tradeability(spread_pct, ask_depth_usd);

    Some(TradeabilityAssessment {
        spread_pct,
        ask_depth_usd,
        bid_depth_usd,
        slippage_100_pct,
        levels_for_100,
        best_ask,
        best_bid,
        rating,
    })
}

/// Rate tradeability based on spread and depth thresholds.
fn rate_tradeability(spread_pct: f64, ask_depth_usd: f64) -> TradeabilityRating {
    if spread_pct <= 0.10 && ask_depth_usd >= 50.0 {
        TradeabilityRating::Good
    } else if spread_pct <= 0.20 && ask_depth_usd >= 20.0 {
        TradeabilityRating::Marginal
    } else {
        TradeabilityRating::Untradeable
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::PriceLevel;

    fn make_book(bids: &[(f64, f64)], asks: &[(f64, f64)]) -> OrderBook {
        // Polymarket format: bids ascending, asks descending
        // Best prices at END of arrays
        OrderBook {
            timestamp: 1000,
            market: "test".to_string(),
            asset_id: "test_token".to_string(),
            bids: bids
                .iter()
                .map(|(p, s)| PriceLevel {
                    price: p.to_string(),
                    size: s.to_string(),
                })
                .collect(),
            asks: asks
                .iter()
                .map(|(p, s)| PriceLevel {
                    price: p.to_string(),
                    size: s.to_string(),
                })
                .collect(),
        }
    }

    #[test]
    fn test_good_tradeability() {
        // Tight spread (3%), decent depth
        let book = make_book(
            &[(0.58, 500.0), (0.59, 300.0), (0.60, 200.0)], // bids ascending, best=0.60
            &[(0.65, 200.0), (0.63, 300.0), (0.62, 500.0)], // asks descending, best=0.62
        );
        let assessment = assess_tradeability(&book).unwrap();
        assert_eq!(assessment.rating, TradeabilityRating::Good);
        assert!(assessment.spread_pct < 0.10);
    }

    #[test]
    fn test_untradeable_wide_spread() {
        // Wide spread (50%)
        let book = make_book(
            &[(0.30, 100.0)], // best bid 0.30
            &[(0.60, 100.0)], // best ask 0.60
        );
        let assessment = assess_tradeability(&book).unwrap();
        assert_eq!(assessment.rating, TradeabilityRating::Untradeable);
    }

    #[test]
    fn test_empty_book() {
        let book = OrderBook {
            timestamp: 1000,
            market: "test".to_string(),
            asset_id: "test_token".to_string(),
            bids: vec![],
            asks: vec![],
        };
        assert!(assess_tradeability(&book).is_none());
    }
}
