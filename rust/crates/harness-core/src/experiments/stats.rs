//! Streaming paired estimates. An independent source cut is one cluster;
//! replicates and arms sharing it are never independent sample units.

use super::Error;
use serde::Serialize;

/// Paired differences belonging to one independent source/problem cluster.
/// Finish a cluster once; callers group the input, so no growing key map exists.
#[derive(Clone, Copy, Debug, Default)]
pub struct Cluster {
    count: u64,
    mean: f64,
    m2: f64,
}

impl Cluster {
    /// Add one treatment-minus-baseline difference in O(1) space and work.
    /// Errors leave this accumulator unchanged.
    pub fn push(&mut self, difference: f64) -> Result<(), Error> {
        finite(difference)?;
        let count = self.count.checked_add(1).ok_or(Error::Overflow)?;
        let delta = difference - self.mean;
        let mean = self.mean + delta / count as f64;
        let m2 = self.m2 + delta * (difference - mean);
        finite(mean)?;
        finite(m2)?;
        *self = Self { count, mean, m2 };
        Ok(())
    }
}

/// Constant-space sufficient statistics, accepting each complete cluster once.
#[derive(Clone, Copy, Debug, Default)]
pub struct PairedStats {
    pairs: Cluster,
    clusters: u64,
    anchor: f64,
    size_norm: f64,
    projected_sum: f64,
    residual_norm: f64,
}

/// Mean over pairs with both ordinary and cluster-robust uncertainty.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct Estimate {
    /// Report format version.
    pub version: u32,
    /// Total matched pairs.
    pub pairs: u64,
    /// Independent cuts/problems; not the number of model calls or agents.
    pub clusters: u64,
    /// Treatment-minus-baseline sample mean, weighted equally per pair.
    pub difference: f64,
    /// Ordinary paired sample standard error; assumes independent pairs.
    pub paired_se: f64,
    /// Intercept-only CR1 sandwich standard error, allowing unequal sizes.
    pub clustered_se: f64,
}

impl PairedStats {
    /// Merge one complete independent cluster without storing its rows.
    /// The independent-cluster assignment is part of the experimental design.
    pub fn push(&mut self, cluster: Cluster) -> Result<(), Error> {
        if cluster.count == 0 {
            return Err(Error::Statistics("empty cluster"));
        }
        let mut next = *self;
        if next.clusters == 0 {
            next.anchor = cluster.mean;
        }
        let pairs = next
            .pairs
            .count
            .checked_add(cluster.count)
            .ok_or(Error::Overflow)?;
        let clusters = next.clusters.checked_add(1).ok_or(Error::Overflow)?;
        let centered_mean = cluster.mean - next.anchor;
        let delta = centered_mean - next.pairs.mean;
        next.pairs.mean += delta * (cluster.count as f64 / pairs as f64);
        next.pairs.m2 += cluster.m2
            + delta * delta * (next.pairs.count as f64 / pairs as f64) * cluster.count as f64;
        next.pairs.count = pairs;
        let sum = centered_mean * cluster.count as f64;
        let size = cluster.count as f64;
        // Online two-column QR of (cluster size, centered cluster sum).
        // Squared residuals are a sum of squares, avoiding subtraction of
        // nearly equal covariance terms for large common treatment offsets.
        let norm = next.size_norm.hypot(size);
        let cosine = next.size_norm / norm;
        let sine = size / norm;
        let residual = cosine * sum - sine * next.projected_sum;
        next.projected_sum = cosine * next.projected_sum + sine * sum;
        next.residual_norm = next.residual_norm.hypot(residual);
        next.size_norm = norm;
        next.clusters = clusters;
        for value in [
            next.pairs.mean,
            next.pairs.m2,
            next.size_norm,
            next.projected_sum,
            next.residual_norm,
        ] {
            finite(value)?;
        }
        *self = next;
        Ok(())
    }

    /// CR1 = G/(G-1) * sum_g (sum_i d_gi - n_g * mean(d))^2 / N^2.
    /// One shared cut cannot estimate cluster uncertainty and is refused.
    pub fn estimate(&self) -> Result<Estimate, Error> {
        if self.clusters < 2 || self.pairs.count < 2 {
            return Err(Error::Statistics(
                "at least two independent clusters are required",
            ));
        }
        let n = self.pairs.count as f64;
        let g = self.clusters as f64;
        let mean = self.pairs.mean;
        let residual = (self.projected_sum - mean * self.size_norm).hypot(self.residual_norm);
        let paired_se = (self.pairs.m2.max(0.0) / (n - 1.0) / n).sqrt();
        let clustered_se = (g / (g - 1.0)).sqrt() * residual / n;
        finite(paired_se)?;
        finite(clustered_se)?;
        Ok(Estimate {
            version: 1,
            pairs: self.pairs.count,
            clusters: self.clusters,
            difference: self.anchor + mean,
            paired_se,
            clustered_se,
        })
    }
}

/// Normal-approximation planning for equal-sized independent cluster means:
/// ceil(((z_(1-alpha/2) + z_power) * cluster_sd / effect)^2).
/// Supply quantiles explicitly (e.g. 1.959964 and 0.841621 for 5%, 80%).
/// This is not a small-sample t calculation or a count of dependent replicates.
pub fn required_clusters(
    cluster_sd: f64,
    effect: f64,
    z_alpha: f64,
    z_power: f64,
) -> Result<u64, Error> {
    for value in [cluster_sd, effect, z_alpha, z_power] {
        finite(value)?;
    }
    if cluster_sd < 0.0 || effect <= 0.0 || z_alpha <= 0.0 || z_power <= 0.0 {
        return Err(Error::Statistics("invalid power-planning inputs"));
    }
    let required = (((z_alpha + z_power) * cluster_sd / effect).powi(2))
        .ceil()
        .max(2.0);
    if !required.is_finite() || required >= u64::MAX as f64 {
        return Err(Error::Overflow);
    }
    Ok(required as u64)
}

fn finite(value: f64) -> Result<(), Error> {
    if value.is_finite() {
        Ok(())
    } else {
        Err(Error::Statistics("nonfinite statistic"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clustered_estimate_matches_direct_residuals_with_unequal_clusters() -> Result<(), Error> {
        let groups: &[&[f64]] = &[&[1.0, 3.0], &[5.0], &[-2.0, 0.0, 2.0]];
        let mut stats = PairedStats::default();
        for group in groups {
            let mut cluster = Cluster::default();
            for &difference in *group {
                cluster.push(difference)?;
            }
            stats.push(cluster)?;
        }
        let estimate = stats.estimate()?;
        let residuals = groups
            .iter()
            .map(|group| (group.iter().sum::<f64>() - group.len() as f64 * 1.5).powi(2))
            .sum::<f64>();
        assert!((estimate.difference - 1.5).abs() < 1e-12);
        assert!((estimate.clustered_se - (1.5 * residuals / 36.0).sqrt()).abs() < 1e-12);
        assert!((estimate.clustered_se - estimate.paired_se).abs() > 0.1);
        let mut shifted = PairedStats::default();
        for group in groups {
            let mut cluster = Cluster::default();
            for &difference in *group {
                cluster.push(difference + 1e12)?;
            }
            shifted.push(cluster)?;
        }
        assert!((shifted.estimate()?.clustered_se - estimate.clustered_se).abs() < 1e-8);
        assert_eq!(required_clusters(1.0, 0.5, 1.959964, 0.841621)?, 32);
        Ok(())
    }

    #[test]
    fn refusal_does_not_mutate_and_dependent_replicates_do_not_create_clusters() -> Result<(), Error>
    {
        let mut cluster = Cluster::default();
        for _ in 0..100 {
            cluster.push(2.0)?;
        }
        let before = cluster;
        assert!(cluster.push(f64::NAN).is_err());
        assert_eq!(cluster.count, before.count);
        assert_eq!(cluster.mean, before.mean);
        let mut stats = PairedStats::default();
        stats.push(cluster)?;
        assert!(stats.estimate().is_err());
        assert!(required_clusters(1.0, 0.0, 1.96, 0.84).is_err());
        assert!(stats.push(Cluster::default()).is_err());
        Ok(())
    }
}
