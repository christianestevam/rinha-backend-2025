// Utilitários para manipulação de valores monetários

pub fn calculate_fee(amount: u64, fee_rate: f64) -> u64 {
    (amount as f64 * fee_rate) as u64
}

pub fn format_currency(amount: u64) -> String {
    format!("${:.2}", amount as f64 / 100.0)
}

pub fn parse_currency(currency_str: &str) -> Result<u64, std::num::ParseFloatError> {
    let cleaned = currency_str.replace('$', "").replace(',', "");
    let float_value: f64 = cleaned.parse()?;
    Ok((float_value * 100.0) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_fee() {
        assert_eq!(calculate_fee(1000, 0.05), 50); // 5% de 1000 = 50
        assert_eq!(calculate_fee(2000, 0.10), 200); // 10% de 2000 = 200
    }

    #[test]
    fn test_format_currency() {
        assert_eq!(format_currency(1000), "$10.00");
        assert_eq!(format_currency(2550), "$25.50");
    }
}