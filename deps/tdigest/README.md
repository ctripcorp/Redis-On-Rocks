# T-Digest

Adaptive histogram based on something like streaming k-means crossed with Q-digest.


This implementation is a descendent of Ted MergingDigest, available at:
[https://github.com/tdunning/t-digest/](https://github.com/tdunning/t-digest/)


And contains the work of  Andrew Werner originally available at:
[https://github.com/ajwerner/tdigestc](https://github.com/ajwerner/tdigestc)

## Description

The t-Digest construction algorithm uses a variant of 1-dimensional
k-means clustering to produce a very compact data structure that allows
accurate estimation of quantiles. This t-Digest data structure can be
used to estimate quantiles, compute other rank statistics or even to
estimate related measures like trimmed means. The advantage of the
t-Digest over previous digests for this purpose is that the t-Digest
handles data with full floating point resolution. The accuracy of
quantile estimates produced by t-Digests can be orders of magnitude more
accurate than those produced by previous digest algorithms. Methods are
provided to create and update t-Digests and retrieve quantiles from the
accumulated distributions.

See [the original paper by Ted Dunning & Otmar
Ertl](https://arxiv.org/abs/1902.04023) for more details on t-Digests.

## What’s Inside

The following functions are implemented:

  - `td_add`: Add a value to the t-Digest with the specified count
  - `td_create`: Allocate a new histogram
  - `td_reset`: Empty out a histogram and re-initialize it
  - `td_free`: Frees the memory associated with the t-Digest
  - `td_compress`: Re-examines a the t-Digest to determine whether some centroids are redundant
  - `td_merge`: Merge one t-Digest into another
  - `td_cdf`:  Returns the fraction of all points added which are &le; x.
  - `td_quantile`: Returns an estimate of the cutoff such that a specified fraction of the data added to the t-Digest would be less than or equal to the cutoff.
  - `td_quantiles`: Returns an estimate of the cutoff such that a specified fraction of the data added to the t-Digest would be less than or equal to the given cutoffs.
  - `td_size`: Return the number of points that have been added to the t-Digest
  - `td_centroid_count`: Return the number of centroids being used by the t-Digest
  - `td_min`: Get the minimum value from the histogram.  Will return __DBL_MAX__ if the histogram is empty
  - `td_max`: Get the maximum value from the histogram.  Will return __DBL_MIN__ if the histogram is empty
  - `td_trimmed_mean`: Returns the trimmed mean ignoring values outside given cutoff upper and lower limits
  - `td_trimmed_mean_symmetric`: Returns the trimmed mean ignoring values outside given a symmetric cutoff limits

## Build notes

``` 
# Build
git clone https://github.com/RedisBloom/t-digest-c.git
cd t-digest-c/
git submodule update --init --recursive
make
```

## Testing 
Assuming you've followed the previous build steps, it should be as easy as:
``` 
# Run the unit tests
make test
```

## Benchmarking

Assuming you've followed the previous build steps, it should be as easy as:
``` 
# Run the benchmark
make bench
```

## Code of Conduct

Please note that this project is released with a Contributor Code of
Conduct. By participating in this project you agree to abide by its
terms.