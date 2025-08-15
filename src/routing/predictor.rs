//! Machine Learning latency predictor for intelligent processor selection
//! 
//! This module implements a lightweight ML system that learns from historical
//! latency patterns to predict which processor will be faster for future requests.

use crate::storage::ProcessorType;
use std::collections::VecDeque;

/// Lightweight machine learning model for latency prediction
pub struct LatencyPredictor {
    /// Historical data samples
    samples: VecDeque<Sample>,
    
    /// Model weights for linear regression
    default_model: LinearModel,
    fallback_model: LinearModel,
    
    /// Feature extractors
    feature_extractors: FeatureExtractors,
    
    /// Model performance tracking
    performance: ModelPerformance,
    
    /// Configuration
    config: PredictorConfig,
}

/// Individual training sample
#[derive(Debug, Clone)]
struct Sample {
    /// Input features
    features: Features,
    /// Actual latency observed (microseconds)
    latency_micros: f32,
    /// Which processor was used
    processor: ProcessorType,
    /// Timestamp when sample was recorded
    timestamp: u64,
    /// Payment amount
    amount: f32,
}

/// Feature vector for ML model
#[derive(Debug, Clone)]
struct Features {
    /// Payment amount (normalized 0-1)
    amount_normalized: f32,
    /// Hour of day (0-23)
    hour_of_day: f32,
    /// Day of week (0-6)
    day_of_week: f32,
    /// Recent average latency
    recent_avg_latency: f32,
    /// Load indicator (requests per second)
    load_indicator: f32,
    /// Time since last failure
    time_since_failure: f32,
    /// Success rate (0-1)
    success_rate: f32,
    /// Trend indicator (-1 to 1)
    trend_indicator: f32,
}

impl Features {
    fn to_array(&self) -> [f32; 8] {
        [
            self.amount_normalized,
            self.hour_of_day,
            self.day_of_week,
            self.recent_avg_latency,
            self.load_indicator,
            self.time_since_failure,
            self.success_rate,
            self.trend_indicator,
        ]
    }
}

/// Simple linear regression model
#[derive(Debug, Clone)]
struct LinearModel {
    /// Model weights (one per feature)
    weights: [f32; 8],
    /// Bias term
    bias: f32,
    /// Learning rate for online learning
    learning_rate: f32,
    /// Number of training samples seen
    samples_seen: u32,
}

impl Default for LinearModel {
    fn default() -> Self {
        Self {
            weights: [0.0; 8],
            bias: 1000.0, // Start with 1ms baseline
            learning_rate: 0.01,
            samples_seen: 0,
        }
    }
}

impl LinearModel {
    /// Predict latency given features
    fn predict(&self, features: &Features) -> f32 {
        let feature_array = features.to_array();
        let mut prediction = self.bias;
        
        for (weight, feature) in self.weights.iter().zip(feature_array.iter()) {
            prediction += weight * feature;
        }
        
        prediction.max(100.0) // Minimum 100μs prediction
    }
    
    /// Update model with new sample (online learning)
    fn update(&mut self, features: &Features, actual_latency: f32) {
        let prediction = self.predict(features);
        let error = actual_latency - prediction;
        
        // Gradient descent update
        let feature_array = features.to_array();
        
        // Update weights
        for (weight, feature) in self.weights.iter_mut().zip(feature_array.iter()) {
            *weight += self.learning_rate * error * feature;
        }
        
        // Update bias
        self.bias += self.learning_rate * error;
        
        // Decay learning rate slightly
        if self.samples_seen % 100 == 0 {
            self.learning_rate *= 0.995;
            self.learning_rate = self.learning_rate.max(0.001); // Minimum learning rate
        }
        
        self.samples_seen += 1;
    }
    
    /// Get model confidence based on training data
    fn get_confidence(&self) -> f32 {
        // Confidence increases with more samples, plateaus at 1000 samples
        let sample_confidence = (self.samples_seen as f32 / 1000.0).min(1.0);
        
        // Factor in weight stability (smaller weights = more confident)
        let weight_magnitude: f32 = self.weights.iter().map(|w| w.abs()).sum();
        let weight_confidence = (1.0 / (1.0 + weight_magnitude * 0.001)).max(0.1);
        
        sample_confidence * weight_confidence
    }
}

/// Feature extraction utilities
struct FeatureExtractors {
    /// Recent latency samples for trend calculation
    recent_latencies: VecDeque<f32>,
    /// Request timestamps for load calculation
    recent_requests: VecDeque<u64>,
    /// Last failure timestamp per processor
    last_failure_default: u64,
    last_failure_fallback: u64,
    /// Success counters
    success_count_default: u32,
    total_count_default: u32,
    success_count_fallback: u32,
    total_count_fallback: u32,
}

impl Default for FeatureExtractors {
    fn default() -> Self {
        Self {
            recent_latencies: VecDeque::with_capacity(100),
            recent_requests: VecDeque::with_capacity(1000),
            last_failure_default: 0,
            last_failure_fallback: 0,
            success_count_default: 0,
            total_count_default: 0,
            success_count_fallback: 0,
            total_count_fallback: 0,
        }
    }
}

impl FeatureExtractors {
    /// Extract features for current context
    fn extract_features(&mut self, amount: f32, processor: ProcessorType) -> Features {
        let now = current_timestamp_millis();
        
        // Time-based features
        let hour_of_day = ((now / 1000 / 3600) % 24) as f32;
        let day_of_week = ((now / 1000 / 86400 + 4) % 7) as f32; // +4 to align with epoch
        
        // Amount normalization (assume max 1000.0)
        let amount_normalized = (amount / 1000.0).min(1.0);
        
        // Recent average latency
        let recent_avg_latency = if self.recent_latencies.is_empty() {
            1000.0 // Default assumption
        } else {
            self.recent_latencies.iter().sum::<f32>() / self.recent_latencies.len() as f32
        };
        
        // Load indicator (requests per second)
        let load_indicator = self.calculate_load_indicator(now);
        
        // Time since last failure
        let time_since_failure = match processor {
            ProcessorType::Default => {
                if self.last_failure_default == 0 {
                    10000.0 // Large value if no failures
                } else {
                    ((now - self.last_failure_default) as f32 / 1000.0).min(10000.0)
                }
            }
            ProcessorType::Fallback => {
                if self.last_failure_fallback == 0 {
                    10000.0
                } else {
                    ((now - self.last_failure_fallback) as f32 / 1000.0).min(10000.0)
                }
            }
        };
        
        // Success rate
        let success_rate = match processor {
            ProcessorType::Default => {
                if self.total_count_default == 0 {
                    1.0
                } else {
                    self.success_count_default as f32 / self.total_count_default as f32
                }
            }
            ProcessorType::Fallback => {
                if self.total_count_fallback == 0 {
                    1.0
                } else {
                    self.success_count_fallback as f32 / self.total_count_fallback as f32
                }
            }
        };
        
        // Trend indicator
        let trend_indicator = self.calculate_trend_indicator();
        
        Features {
            amount_normalized,
            hour_of_day,
            day_of_week,
            recent_avg_latency,
            load_indicator,
            time_since_failure,
            success_rate,
            trend_indicator,
        }
    }
    
    /// Update internal state with new sample
    fn update_state(&mut self, processor: ProcessorType, success: bool, latency: f32) {
        let now = current_timestamp_millis();
        
        // Update latency history
        self.recent_latencies.push_back(latency);
        if self.recent_latencies.len() > 100 {
            self.recent_latencies.pop_front();
        }
        
        // Update request history for load calculation
        self.recent_requests.push_back(now);
        // Keep only last 10 seconds of requests
        while let Some(&front_time) = self.recent_requests.front() {
            if now - front_time > 10000 { // 10 seconds
                self.recent_requests.pop_front();
            } else {
                break;
            }
        }
        
        // Update success/failure tracking
        match processor {
            ProcessorType::Default => {
                self.total_count_default += 1;
                if success {
                    self.success_count_default += 1;
                } else {
                    self.last_failure_default = now;
                }
            }
            ProcessorType::Fallback => {
                self.total_count_fallback += 1;
                if success {
                    self.success_count_fallback += 1;
                } else {
                    self.last_failure_fallback = now;
                }
            }
        }
    }
    
    /// Calculate current load indicator
    fn calculate_load_indicator(&self, now: u64) -> f32 {
        // Count requests in last second
        let requests_in_last_second = self.recent_requests
            .iter()
            .filter(|&&timestamp| now - timestamp <= 1000)
            .count();
        
        // Normalize to 0-1 range (assume max 1000 RPS)
        (requests_in_last_second as f32 / 1000.0).min(1.0)
    }
    
    /// Calculate trend indicator (-1 = getting worse, 0 = stable, 1 = getting better)
    fn calculate_trend_indicator(&self) -> f32 {
        if self.recent_latencies.len() < 10 {
            return 0.0; // Not enough data
        }
        
        let recent_half = self.recent_latencies.len() / 2;
        let old_half: f32 = self.recent_latencies.iter().take(recent_half).sum();
        let new_half: f32 = self.recent_latencies.iter().skip(recent_half).sum();
        
        let old_avg = old_half / recent_half as f32;
        let new_avg = new_half / (self.recent_latencies.len() - recent_half) as f32;
        
        // Calculate trend (-1 to 1)
        let trend = (old_avg - new_avg) / old_avg.max(new_avg).max(1.0);
        trend.max(-1.0).min(1.0)
    }
}

/// Model performance tracking
#[derive(Default)]
struct ModelPerformance {
    /// Prediction errors for accuracy tracking
    default_errors: VecDeque<f32>,
    fallback_errors: VecDeque<f32>,
    
    /// Total predictions made
    total_predictions: u32,
    correct_processor_choices: u32,
}

impl ModelPerformance {
    /// Record prediction accuracy
    fn record_prediction(&mut self, processor: ProcessorType, predicted: f32, actual: f32) {
        let error = (predicted - actual).abs();
        
        match processor {
            ProcessorType::Default => {
                self.default_errors.push_back(error);
                if self.default_errors.len() > 100 {
                    self.default_errors.pop_front();
                }
            }
            ProcessorType::Fallback => {
                self.fallback_errors.push_back(error);
                if self.fallback_errors.len() > 100 {
                    self.fallback_errors.pop_front();
                }
            }
        }
    }
    
    /// Get model accuracy metrics
    fn get_accuracy(&self) -> (f32, f32) {
        let default_mae = if self.default_errors.is_empty() {
            0.0
        } else {
            self.default_errors.iter().sum::<f32>() / self.default_errors.len() as f32
        };
        
        let fallback_mae = if self.fallback_errors.is_empty() {
            0.0
        } else {
            self.fallback_errors.iter().sum::<f32>() / self.fallback_errors.len() as f32
        };
        
        (default_mae, fallback_mae)
    }
    
    /// Get processor choice accuracy
    fn get_choice_accuracy(&self) -> f32 {
        if self.total_predictions == 0 {
            0.0
        } else {
            self.correct_processor_choices as f32 / self.total_predictions as f32
        }
    }
}

/// Predictor configuration
#[derive(Debug, Clone)]
pub struct PredictorConfig {
    /// Maximum number of historical samples to keep
    pub max_samples: usize,
    /// Minimum samples required before making predictions
    pub min_samples_for_prediction: usize,
    /// Feature update frequency
    pub feature_update_interval_ms: u64,
    /// Model retraining frequency
    pub retrain_interval_samples: u32,
}

impl Default for PredictorConfig {
    fn default() -> Self {
        Self {
            max_samples: 10000,
            min_samples_for_prediction: 50,
            feature_update_interval_ms: 1000,
            retrain_interval_samples: 100,
        }
    }
}

impl LatencyPredictor {
    /// Create new latency predictor
    pub fn new(max_samples: usize) -> Self {
        let config = PredictorConfig {
            max_samples,
            ..Default::default()
        };
        
        Self {
            samples: VecDeque::with_capacity(config.max_samples),
            default_model: LinearModel::default(),
            fallback_model: LinearModel::default(),
            feature_extractors: FeatureExtractors::default(),
            performance: ModelPerformance::default(),
            config,
        }
    }
    
    /// Add new training sample
    pub fn add_sample(&mut self, processor: ProcessorType, latency_micros: f32, amount: f32) {
        let features = self.feature_extractors.extract_features(amount, processor);
        
        let sample = Sample {
            features: features.clone(),
            latency_micros,
            processor,
            timestamp: current_timestamp_millis(),
            amount,
        };
        
        // Add to sample history
        self.samples.push_back(sample);
        if self.samples.len() > self.config.max_samples {
            self.samples.pop_front();
        }
        
        // Update feature extractors
        self.feature_extractors.update_state(processor, true, latency_micros);
        
        // Update appropriate model
        match processor {
            ProcessorType::Default => {
                self.default_model.update(&features, latency_micros);
            }
            ProcessorType::Fallback => {
                self.fallback_model.update(&features, latency_micros);
            }
        }
        
        // Record prediction accuracy if we have a previous prediction
        self.performance.record_prediction(processor, self.predict_latency(processor, amount), latency_micros);
        
        // Periodic retraining
        if self.samples.len() % self.config.retrain_interval_samples as usize == 0 {
            self.retrain_models();
        }
    }
    
    /// Predict latency for specific processor and amount
    pub fn predict_latency(&self, processor: ProcessorType, amount: f32) -> f32 {
        if self.samples.len() < self.config.min_samples_for_prediction {
            // Not enough data, return baseline estimates
            return match processor {
                ProcessorType::Default => 1000.0,  // 1ms
                ProcessorType::Fallback => 2000.0, // 2ms
            };
        }
        
        let mut features = self.feature_extractors.extract_features(amount, processor);
        
        match processor {
            ProcessorType::Default => self.default_model.predict(&features),
            ProcessorType::Fallback => self.fallback_model.predict(&features),
        }
    }
    
    /// Predict latencies for both processors
    pub fn predict_latencies(&self, amount: f32) -> (f32, f32) {
        (
            self.predict_latency(ProcessorType::Default, amount),
            self.predict_latency(ProcessorType::Fallback, amount),
        )
    }
    
    /// Get prediction confidence (0.0 to 1.0)
    pub fn get_prediction_confidence(&self) -> f64 {
        if self.samples.len() < self.config.min_samples_for_prediction {
            return 0.0;
        }
        
        let default_confidence = self.default_model.get_confidence();
        let fallback_confidence = self.fallback_model.get_confidence();
        
        // Average confidence, weighted by sample count
        let default_weight = self.default_model.samples_seen as f32;
        let fallback_weight = self.fallback_model.samples_seen as f32;
        let total_weight = default_weight + fallback_weight;
        
        if total_weight == 0.0 {
            0.0
        } else {
            let weighted_confidence = (default_confidence * default_weight + 
                                     fallback_confidence * fallback_weight) / total_weight;
            weighted_confidence as f64
        }
    }
    
    /// Get comprehensive prediction metrics
    pub fn get_metrics(&self) -> PredictionMetrics {
        let (default_mae, fallback_mae) = self.performance.get_accuracy();
        let choice_accuracy = self.performance.get_choice_accuracy();
        
        PredictionMetrics {
            total_samples: self.samples.len(),
            default_model_samples: self.default_model.samples_seen,
            fallback_model_samples: self.fallback_model.samples_seen,
            prediction_confidence: self.get_prediction_confidence(),
            default_mae,
            fallback_mae,
            choice_accuracy,
            is_ready_for_predictions: self.samples.len() >= self.config.min_samples_for_prediction,
        }
    }
    
    /// Reset predictor state
    pub fn reset(&mut self) {
        self.samples.clear();
        self.default_model = LinearModel::default();
        self.fallback_model = LinearModel::default();
        self.feature_extractors = FeatureExtractors::default();
        self.performance = ModelPerformance::default();
    }
    
    /// Retrain models on all historical data
    fn retrain_models(&mut self) {
        // Reset models
        self.default_model = LinearModel::default();
        self.fallback_model = LinearModel::default();
        
        // Retrain on all samples
        for sample in &self.samples {
            match sample.processor {
                ProcessorType::Default => {
                    self.default_model.update(&sample.features, sample.latency_micros);
                }
                ProcessorType::Fallback => {
                    self.fallback_model.update(&sample.features, sample.latency_micros);
                }
            }
        }
    }
    
    /// Export model state for analysis
    pub fn export_model_state(&self) -> ModelState {
        ModelState {
            default_weights: self.default_model.weights,
            default_bias: self.default_model.bias,
            fallback_weights: self.fallback_model.weights,
            fallback_bias: self.fallback_model.bias,
            sample_count: self.samples.len(),
        }
    }
}

/// Prediction metrics for monitoring
#[derive(Debug, Clone)]
pub struct PredictionMetrics {
    pub total_samples: usize,
    pub default_model_samples: u32,
    pub fallback_model_samples: u32,
    pub prediction_confidence: f64,
    pub default_mae: f32,
    pub fallback_mae: f32,
    pub choice_accuracy: f32,
    pub is_ready_for_predictions: bool,
}

/// Model state for analysis
#[derive(Debug, Clone)]
pub struct ModelState {
    pub default_weights: [f32; 8],
    pub default_bias: f32,
    pub fallback_weights: [f32; 8],
    pub fallback_bias: f32,
    pub sample_count: usize,
}

/// Get current timestamp in milliseconds
fn current_timestamp_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_latency_predictor_basic() {
        let mut predictor = LatencyPredictor::new(1000);
        
        // Add some training samples
        predictor.add_sample(ProcessorType::Default, 1000.0, 19.90);
        predictor.add_sample(ProcessorType::Default, 1100.0, 29.90);
        predictor.add_sample(ProcessorType::Fallback, 2000.0, 19.90);
        predictor.add_sample(ProcessorType::Fallback, 2200.0, 29.90);
        
        let metrics = predictor.get_metrics();
        assert_eq!(metrics.total_samples, 4);
        assert_eq!(metrics.default_model_samples, 2);
        assert_eq!(metrics.fallback_model_samples, 2);
    }
    
    #[test]
    fn test_feature_extraction() {
        let mut extractors = FeatureExtractors::default();
        
        let features = extractors.extract_features(19.90, ProcessorType::Default);
        
        // Check feature ranges
        assert!(features.amount_normalized >= 0.0 && features.amount_normalized <= 1.0);
        assert!(features.hour_of_day >= 0.0 && features.hour_of_day <= 23.0);
        assert!(features.success_rate >= 0.0 && features.success_rate <= 1.0);
    }
    
    #[test]
    fn test_linear_model_training() {
        let mut model = LinearModel::default();
        
        let features = Features {
            amount_normalized: 0.5,
            hour_of_day: 12.0,
            day_of_week: 3.0,
            recent_avg_latency: 1000.0,
            load_indicator: 0.3,
            time_since_failure: 1000.0,
            success_rate: 0.95,
            trend_indicator: 0.1,
        };
        
        // Train with some samples
        model.update(&features, 1200.0);
        model.update(&features, 1100.0);
        model.update(&features, 1150.0);
        
        // Check prediction is reasonable
        let prediction = model.predict(&features);
        assert!(prediction > 100.0 && prediction < 10000.0);
        
        // Check confidence increases with samples
        assert!(model.get_confidence() > 0.0);
    }
    
    #[test]
    fn test_prediction_with_insufficient_data() {
        let predictor = LatencyPredictor::new(1000);
        
        // Should return baseline predictions
        let default_pred = predictor.predict_latency(ProcessorType::Default, 19.90);
        let fallback_pred = predictor.predict_latency(ProcessorType::Fallback, 19.90);
        
        assert_eq!(default_pred, 1000.0);
        assert_eq!(fallback_pred, 2000.0);
        assert_eq!(predictor.get_prediction_confidence(), 0.0);
    }
    
    #[test]
    fn test_prediction_with_sufficient_data() {
        let mut predictor = LatencyPredictor::new(1000);
        
        // Add enough samples
        for i in 0..100 {
            let amount = (i % 10) as f32 * 10.0;
            predictor.add_sample(ProcessorType::Default, 1000.0 + (i % 5) as f32 * 100.0, amount);
            predictor.add_sample(ProcessorType::Fallback, 2000.0 + (i % 3) as f32 * 200.0, amount);
        }
        
        let metrics = predictor.get_metrics();
        assert!(metrics.is_ready_for_predictions);
        assert!(metrics.prediction_confidence > 0.0);
        
        // Predictions should be different from baseline
        let (default_pred, fallback_pred) = predictor.predict_latencies(25.0);
        assert_ne!(default_pred, 1000.0);
        assert_ne!(fallback_pred, 2000.0);
    }
    
    #[test]
    fn test_model_retraining() {
        let mut predictor = LatencyPredictor::new(1000);
        
        // Add samples to trigger retraining
        for i in 0..150 {
            predictor.add_sample(ProcessorType::Default, 1000.0 + (i as f32), 19.90);
        }
        
        let metrics = predictor.get_metrics();
        assert!(metrics.default_model_samples > 0);
        
        // Export state for analysis
        let state = predictor.export_model_state();
        assert_eq!(state.sample_count, 150);
    }
}