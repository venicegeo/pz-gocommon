// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package elastic

import (
	"encoding/json"
	"testing"
)

func TestAggsMetricsMin(t *testing.T) {
	s := `{
	"min_price": {
  	"value": 10
  }
}`

	aggs := new(Aggregations)
	err := json.Unmarshal([]byte(s), &aggs)
	if err != nil {
		t.Fatalf("expected no error decoding; got: %v", err)
	}

	agg, found := aggs.Min("min_price")
	if !found {
		t.Fatalf("expected aggregation to be found; got: %v", found)
	}
	if agg == nil {
		t.Fatalf("expected aggregation != nil; got: %v", agg)
	}
	if agg.Value == nil {
		t.Fatalf("expected aggregation value != nil; got: %v", agg.Value)
	}
	if *agg.Value != float64(10) {
		t.Fatalf("expected aggregation value = %v; got: %v", float64(10), *agg.Value)
	}
}

func TestAggsMetricsMax(t *testing.T) {
	s := `{
	"max_price": {
  	"value": 35
  }
}`

	aggs := new(Aggregations)
	err := json.Unmarshal([]byte(s), &aggs)
	if err != nil {
		t.Fatalf("expected no error decoding; got: %v", err)
	}

	agg, found := aggs.Max("max_price")
	if !found {
		t.Fatalf("expected aggregation to be found; got: %v", found)
	}
	if agg == nil {
		t.Fatalf("expected aggregation != nil; got: %v", agg)
	}
	if agg.Value == nil {
		t.Fatalf("expected aggregation value != nil; got: %v", agg.Value)
	}
	if *agg.Value != float64(35) {
		t.Fatalf("expected aggregation value = %v; got: %v", float64(35), *agg.Value)
	}
}

func TestAggsMetricsSum(t *testing.T) {
	s := `{
	"intraday_return": {
  	"value": 2.18
  }
}`

	aggs := new(Aggregations)
	err := json.Unmarshal([]byte(s), &aggs)
	if err != nil {
		t.Fatalf("expected no error decoding; got: %v", err)
	}

	agg, found := aggs.Sum("intraday_return")
	if !found {
		t.Fatalf("expected aggregation to be found; got: %v", found)
	}
	if agg == nil {
		t.Fatalf("expected aggregation != nil; got: %v", agg)
	}
	if agg.Value == nil {
		t.Fatalf("expected aggregation value != nil; got: %v", agg.Value)
	}
	if *agg.Value != float64(2.18) {
		t.Fatalf("expected aggregation value = %v; got: %v", float64(2.18), *agg.Value)
	}
}

func TestAggsMetricsAvg(t *testing.T) {
	s := `{
	"avg_grade": {
  	"value": 75
  }
}`

	aggs := new(Aggregations)
	err := json.Unmarshal([]byte(s), &aggs)
	if err != nil {
		t.Fatalf("expected no error decoding; got: %v", err)
	}

	agg, found := aggs.Avg("avg_grade")
	if !found {
		t.Fatalf("expected aggregation to be found; got: %v", found)
	}
	if agg == nil {
		t.Fatalf("expected aggregation != nil; got: %v", agg)
	}
	if agg.Value == nil {
		t.Fatalf("expected aggregation value != nil; got: %v", agg.Value)
	}
	if *agg.Value != float64(75) {
		t.Fatalf("expected aggregation value = %v; got: %v", float64(75), *agg.Value)
	}
}

func TestAggsMetricsValueCount(t *testing.T) {
	s := `{
	"grades_count": {
  	"value": 10
  }
}`

	aggs := new(Aggregations)
	err := json.Unmarshal([]byte(s), &aggs)
	if err != nil {
		t.Fatalf("expected no error decoding; got: %v", err)
	}

	agg, found := aggs.ValueCount("grades_count")
	if !found {
		t.Fatalf("expected aggregation to be found; got: %v", found)
	}
	if agg == nil {
		t.Fatalf("expected aggregation != nil; got: %v", agg)
	}
	if agg.Value == nil {
		t.Fatalf("expected aggregation value != nil; got: %v", agg.Value)
	}
	if *agg.Value != float64(10) {
		t.Fatalf("expected aggregation value = %v; got: %v", float64(10), *agg.Value)
	}
}

func TestAggsMetricsCardinality(t *testing.T) {
	s := `{
	"author_count": {
  	"value": 12
  }
}`

	aggs := new(Aggregations)
	err := json.Unmarshal([]byte(s), &aggs)
	if err != nil {
		t.Fatalf("expected no error decoding; got: %v", err)
	}

	agg, found := aggs.Cardinality("author_count")
	if !found {
		t.Fatalf("expected aggregation to be found; got: %v", found)
	}
	if agg == nil {
		t.Fatalf("expected aggregation != nil; got: %v", agg)
	}
	if agg.Value == nil {
		t.Fatalf("expected aggregation value != nil; got: %v", agg.Value)
	}
	if *agg.Value != float64(12) {
		t.Fatalf("expected aggregation value = %v; got: %v", float64(12), *agg.Value)
	}
}

func TestAggsMetricsStats(t *testing.T) {
	s := `{
	"grades_stats": {
    "count": 6,
    "min": 60,
    "max": 98,
    "avg": 78.5,
    "sum": 471
  }
}`

	aggs := new(Aggregations)
	err := json.Unmarshal([]byte(s), &aggs)
	if err != nil {
		t.Fatalf("expected no error decoding; got: %v", err)
	}

	agg, found := aggs.Stats("grades_stats")
	if !found {
		t.Fatalf("expected aggregation to be found; got: %v", found)
	}
	if agg == nil {
		t.Fatalf("expected aggregation != nil; got: %v", agg)
	}
	if agg.Count != int64(6) {
		t.Fatalf("expected aggregation Count = %v; got: %v", int64(6), agg.Count)
	}
	if agg.Min == nil {
		t.Fatalf("expected aggregation Min != nil; got: %v", agg.Min)
	}
	if *agg.Min != float64(60) {
		t.Fatalf("expected aggregation Min = %v; got: %v", float64(60), *agg.Min)
	}
	if agg.Max == nil {
		t.Fatalf("expected aggregation Max != nil; got: %v", agg.Max)
	}
	if *agg.Max != float64(98) {
		t.Fatalf("expected aggregation Max = %v; got: %v", float64(98), *agg.Max)
	}
	if agg.Avg == nil {
		t.Fatalf("expected aggregation Avg != nil; got: %v", agg.Avg)
	}
	if *agg.Avg != float64(78.5) {
		t.Fatalf("expected aggregation Avg = %v; got: %v", float64(78.5), *agg.Avg)
	}
	if agg.Sum == nil {
		t.Fatalf("expected aggregation Sum != nil; got: %v", agg.Sum)
	}
	if *agg.Sum != float64(471) {
		t.Fatalf("expected aggregation Sum = %v; got: %v", float64(471), *agg.Sum)
	}
}

func TestAggsMetricsExtendedStats(t *testing.T) {
	s := `{
	"grades_stats": {
    "count": 6,
    "min": 72,
    "max": 117.6,
    "avg": 94.2,
    "sum": 565.2,
    "sum_of_squares": 54551.51999999999,
    "variance": 218.2799999999976,
    "std_deviation": 14.774302013969987
  }
}`

	aggs := new(Aggregations)
	err := json.Unmarshal([]byte(s), &aggs)
	if err != nil {
		t.Fatalf("expected no error decoding; got: %v", err)
	}

	agg, found := aggs.ExtendedStats("grades_stats")
	if !found {
		t.Fatalf("expected aggregation to be found; got: %v", found)
	}
	if agg == nil {
		t.Fatalf("expected aggregation != nil; got: %v", agg)
	}
	if agg.Count != int64(6) {
		t.Fatalf("expected aggregation Count = %v; got: %v", int64(6), agg.Count)
	}
	if agg.Min == nil {
		t.Fatalf("expected aggregation Min != nil; got: %v", agg.Min)
	}
	if *agg.Min != float64(72) {
		t.Fatalf("expected aggregation Min = %v; got: %v", float64(72), *agg.Min)
	}
	if agg.Max == nil {
		t.Fatalf("expected aggregation Max != nil; got: %v", agg.Max)
	}
	if *agg.Max != float64(117.6) {
		t.Fatalf("expected aggregation Max = %v; got: %v", float64(117.6), *agg.Max)
	}
	if agg.Avg == nil {
		t.Fatalf("expected aggregation Avg != nil; got: %v", agg.Avg)
	}
	if *agg.Avg != float64(94.2) {
		t.Fatalf("expected aggregation Avg = %v; got: %v", float64(94.2), *agg.Avg)
	}
	if agg.Sum == nil {
		t.Fatalf("expected aggregation Sum != nil; got: %v", agg.Sum)
	}
	if *agg.Sum != float64(565.2) {
		t.Fatalf("expected aggregation Sum = %v; got: %v", float64(565.2), *agg.Sum)
	}
	if agg.SumOfSquares == nil {
		t.Fatalf("expected aggregation sum_of_squares != nil; got: %v", agg.SumOfSquares)
	}
	if *agg.SumOfSquares != float64(54551.51999999999) {
		t.Fatalf("expected aggregation sum_of_squares = %v; got: %v", float64(54551.51999999999), *agg.SumOfSquares)
	}
	if agg.Variance == nil {
		t.Fatalf("expected aggregation Variance != nil; got: %v", agg.Variance)
	}
	if *agg.Variance != float64(218.2799999999976) {
		t.Fatalf("expected aggregation Variance = %v; got: %v", float64(218.2799999999976), *agg.Variance)
	}
	if agg.StdDeviation == nil {
		t.Fatalf("expected aggregation StdDeviation != nil; got: %v", agg.StdDeviation)
	}
	if *agg.StdDeviation != float64(14.774302013969987) {
		t.Fatalf("expected aggregation StdDeviation = %v; got: %v", float64(14.774302013969987), *agg.StdDeviation)
	}
}

func TestAggsMetricsPercentiles(t *testing.T) {
	s := `{
  "load_time_outlier": {
		"values" : {
		  "1.0": 15,
		  "5.0": 20,
		  "25.0": 23,
		  "50.0": 25,
		  "75.0": 29,
		  "95.0": 60,
		  "99.0": 150
		}
  }
}`

	aggs := new(Aggregations)
	err := json.Unmarshal([]byte(s), &aggs)
	if err != nil {
		t.Fatalf("expected no error decoding; got: %v", err)
	}

	agg, found := aggs.Percentiles("load_time_outlier")
	if !found {
		t.Fatalf("expected aggregation to be found; got: %v", found)
	}
	if agg == nil {
		t.Fatalf("expected aggregation != nil; got: %v", agg)
	}
	if agg.Values == nil {
		t.Fatalf("expected aggregation Values != nil; got: %v", agg.Values)
	}
	if len(agg.Values) != 7 {
		t.Fatalf("expected %d aggregation Values; got: %d", 7, len(agg.Values))
	}
	if agg.Values["1.0"] != float64(15) {
		t.Errorf("expected aggregation value for \"1.0\" = %v; got: %v", float64(15), agg.Values["1.0"])
	}
	if agg.Values["5.0"] != float64(20) {
		t.Errorf("expected aggregation value for \"5.0\" = %v; got: %v", float64(20), agg.Values["5.0"])
	}
	if agg.Values["25.0"] != float64(23) {
		t.Errorf("expected aggregation value for \"25.0\" = %v; got: %v", float64(23), agg.Values["25.0"])
	}
	if agg.Values["50.0"] != float64(25) {
		t.Errorf("expected aggregation value for \"50.0\" = %v; got: %v", float64(25), agg.Values["50.0"])
	}
	if agg.Values["75.0"] != float64(29) {
		t.Errorf("expected aggregation value for \"75.0\" = %v; got: %v", float64(29), agg.Values["75.0"])
	}
	if agg.Values["95.0"] != float64(60) {
		t.Errorf("expected aggregation value for \"95.0\" = %v; got: %v", float64(60), agg.Values["95.0"])
	}
	if agg.Values["99.0"] != float64(150) {
		t.Errorf("expected aggregation value for \"99.0\" = %v; got: %v", float64(150), agg.Values["99.0"])
	}
}

func TestAggsMetricsPercentileRanks(t *testing.T) {
	s := `{
  "load_time_outlier": {
		"values" : {
		  "15": 92,
		  "30": 100
		}
  }
}`

	aggs := new(Aggregations)
	err := json.Unmarshal([]byte(s), &aggs)
	if err != nil {
		t.Fatalf("expected no error decoding; got: %v", err)
	}

	agg, found := aggs.PercentileRanks("load_time_outlier")
	if !found {
		t.Fatalf("expected aggregation to be found; got: %v", found)
	}
	if agg == nil {
		t.Fatalf("expected aggregation != nil; got: %v", agg)
	}
	if agg.Values == nil {
		t.Fatalf("expected aggregation Values != nil; got: %v", agg.Values)
	}
	if len(agg.Values) != 2 {
		t.Fatalf("expected %d aggregation Values; got: %d", 7, len(agg.Values))
	}
	if agg.Values["15"] != float64(92) {
		t.Errorf("expected aggregation value for \"15\" = %v; got: %v", float64(92), agg.Values["15"])
	}
	if agg.Values["30"] != float64(100) {
		t.Errorf("expected aggregation value for \"30\" = %v; got: %v", float64(100), agg.Values["30"])
	}
}

func TestAggsMetricsTopHits(t *testing.T) {
	s := `{
  "top-tags": {
     "buckets": [
        {
           "key": "windows-7",
           "doc_count": 25365,
           "top_tags_hits": {
              "hits": {
                 "total": 25365,
                 "max_score": 1,
                 "hits": [
                    {
                       "_index": "stack",
                       "_type": "question",
                       "_id": "602679",
                       "_score": 1,
                       "_source": {
                          "title": "Windows port opening"
                       },
                       "sort": [
                          1370143231177
                       ]
                    }
                 ]
              }
           }
        },
        {
           "key": "linux",
           "doc_count": 18342,
           "top_tags_hits": {
              "hits": {
                 "total": 18342,
                 "max_score": 1,
                 "hits": [
                    {
                       "_index": "stack",
                       "_type": "question",
                       "_id": "602672",
                       "_score": 1,
                       "_source": {
                          "title": "Ubuntu RFID Screensaver lock-unlock"
                       },
                       "sort": [
                          1370143379747
                       ]
                    }
                 ]
              }
           }
        },
        {
           "key": "windows",
           "doc_count": 18119,
           "top_tags_hits": {
              "hits": {
                 "total": 18119,
                 "max_score": 1,
                 "hits": [
                    {
                       "_index": "stack",
                       "_type": "question",
                       "_id": "602678",
                       "_score": 1,
                       "_source": {
                          "title": "If I change my computers date / time, what could be affected?"
                       },
                       "sort": [
                          1370142868283
                       ]
                    }
                 ]
              }
           }
        }
     ]
  }
}`

	aggs := new(Aggregations)
	err := json.Unmarshal([]byte(s), &aggs)
	if err != nil {
		t.Fatalf("expected no error decoding; got: %v", err)
	}

	agg, found := aggs.Terms("top-tags")
	if !found {
		t.Fatalf("expected aggregation to be found; got: %v", found)
	}
	if agg == nil {
		t.Fatalf("expected aggregation != nil; got: %v", agg)
	}
	if agg.Buckets == nil {
		t.Fatalf("expected aggregation buckets != nil; got: %v", agg.Buckets)
	}
	if len(agg.Buckets) != 3 {
		t.Errorf("expected %d bucket entries; got: %d", 3, len(agg.Buckets))
	}
	if agg.Buckets[0].Key != "windows-7" {
		t.Errorf("expected bucket key = %q; got: %q", "windows-7", agg.Buckets[0].Key)
	}
	if agg.Buckets[1].Key != "linux" {
		t.Errorf("expected bucket key = %q; got: %q", "linux", agg.Buckets[1].Key)
	}
	if agg.Buckets[2].Key != "windows" {
		t.Errorf("expected bucket key = %q; got: %q", "windows", agg.Buckets[2].Key)
	}

	// Sub-aggregation of top-hits
	subAgg, found := agg.Buckets[0].TopHits("top_tags_hits")
	if !found {
		t.Fatalf("expected sub aggregation to be found; got: %v", found)
	}
	if subAgg == nil {
		t.Fatalf("expected sub aggregation != nil; got: %v", subAgg)
	}
	if subAgg.Hits == nil {
		t.Fatalf("expected sub aggregation Hits != nil; got: %v", subAgg.Hits)
	}
	if subAgg.Hits.TotalHits != 25365 {
		t.Fatalf("expected sub aggregation Hits.TotalHits = %d; got: %d", 25365, subAgg.Hits.TotalHits)
	}
	if subAgg.Hits.MaxScore == nil {
		t.Fatalf("expected sub aggregation Hits.MaxScore != %v; got: %v", nil, *subAgg.Hits.MaxScore)
	}
	if *subAgg.Hits.MaxScore != float64(1.0) {
		t.Fatalf("expected sub aggregation Hits.MaxScore = %v; got: %v", float64(1.0), *subAgg.Hits.MaxScore)
	}

	subAgg, found = agg.Buckets[1].TopHits("top_tags_hits")
	if !found {
		t.Fatalf("expected sub aggregation to be found; got: %v", found)
	}
	if subAgg == nil {
		t.Fatalf("expected sub aggregation != nil; got: %v", subAgg)
	}
	if subAgg.Hits == nil {
		t.Fatalf("expected sub aggregation Hits != nil; got: %v", subAgg.Hits)
	}
	if subAgg.Hits.TotalHits != 18342 {
		t.Fatalf("expected sub aggregation Hits.TotalHits = %d; got: %d", 18342, subAgg.Hits.TotalHits)
	}
	if subAgg.Hits.MaxScore == nil {
		t.Fatalf("expected sub aggregation Hits.MaxScore != %v; got: %v", nil, *subAgg.Hits.MaxScore)
	}
	if *subAgg.Hits.MaxScore != float64(1.0) {
		t.Fatalf("expected sub aggregation Hits.MaxScore = %v; got: %v", float64(1.0), *subAgg.Hits.MaxScore)
	}

	subAgg, found = agg.Buckets[2].TopHits("top_tags_hits")
	if !found {
		t.Fatalf("expected sub aggregation to be found; got: %v", found)
	}
	if subAgg == nil {
		t.Fatalf("expected sub aggregation != nil; got: %v", subAgg)
	}
	if subAgg.Hits == nil {
		t.Fatalf("expected sub aggregation Hits != nil; got: %v", subAgg.Hits)
	}
	if subAgg.Hits.TotalHits != 18119 {
		t.Fatalf("expected sub aggregation Hits.TotalHits = %d; got: %d", 18119, subAgg.Hits.TotalHits)
	}
	if subAgg.Hits.MaxScore == nil {
		t.Fatalf("expected sub aggregation Hits.MaxScore != %v; got: %v", nil, *subAgg.Hits.MaxScore)
	}
	if *subAgg.Hits.MaxScore != float64(1.0) {
		t.Fatalf("expected sub aggregation Hits.MaxScore = %v; got: %v", float64(1.0), *subAgg.Hits.MaxScore)
	}
}

func TestAggsBucketGlobal(t *testing.T) {
	s := `{
	"all_products" : {
    "doc_count" : 100,
		"avg_price" : {
			"value" : 56.3
		}
	}
}`

	aggs := new(Aggregations)
	err := json.Unmarshal([]byte(s), &aggs)
	if err != nil {
		t.Fatalf("expected no error decoding; got: %v", err)
	}

	agg, found := aggs.Global("all_products")
	if !found {
		t.Fatalf("expected aggregation to be found; got: %v", found)
	}
	if agg == nil {
		t.Fatalf("expected aggregation != nil; got: %v", agg)
	}
	if agg.DocCount != 100 {
		t.Fatalf("expected aggregation DocCount = %d; got: %d", 100, agg.DocCount)
	}

	// Sub-aggregation
	subAgg, found := agg.Avg("avg_price")
	if !found {
		t.Fatalf("expected sub-aggregation to be found; got: %v", found)
	}
	if subAgg == nil {
		t.Fatalf("expected sub-aggregation != nil; got: %v", subAgg)
	}
	if subAgg.Value == nil {
		t.Fatalf("expected sub-aggregation value != nil; got: %v", subAgg.Value)
	}
	if *subAgg.Value != float64(56.3) {
		t.Fatalf("expected sub-aggregation value = %v; got: %v", float64(56.3), *subAgg.Value)
	}
}

func TestAggsBucketFilter(t *testing.T) {
	s := `{
	"in_stock_products" : {
	  "doc_count" : 100,
	  "avg_price" : { "value" : 56.3 }
	}
}`

	aggs := new(Aggregations)
	err := json.Unmarshal([]byte(s), &aggs)
	if err != nil {
		t.Fatalf("expected no error decoding; got: %v", err)
	}

	agg, found := aggs.Filter("in_stock_products")
	if !found {
		t.Fatalf("expected aggregation to be found; got: %v", found)
	}
	if agg == nil {
		t.Fatalf("expected aggregation != nil; got: %v", agg)
	}
	if agg.DocCount != 100 {
		t.Fatalf("expected aggregation DocCount = %d; got: %d", 100, agg.DocCount)
	}

	// Sub-aggregation
	subAgg, found := agg.Avg("avg_price")
	if !found {
		t.Fatalf("expected sub-aggregation to be found; got: %v", found)
	}
	if subAgg == nil {
		t.Fatalf("expected sub-aggregation != nil; got: %v", subAgg)
	}
	if subAgg.Value == nil {
		t.Fatalf("expected sub-aggregation value != nil; got: %v", subAgg.Value)
	}
	if *subAgg.Value != float64(56.3) {
		t.Fatalf("expected sub-aggregation value = %v; got: %v", float64(56.3), *subAgg.Value)
	}
}

func TestAggsBucketFiltersWithBuckets(t *testing.T) {
	s := `{
  "messages" : {
    "buckets" : [
    	{
        "doc_count" : 34,
        "monthly" : {
          "buckets" : []
        }
      },
      {
        "doc_count" : 439,
        "monthly" : {
          "buckets" : []
        }
      }
    ]
  }
}`

	aggs := new(Aggregations)
	err := json.Unmarshal([]byte(s), &aggs)
	if err != nil {
		t.Fatalf("expected no error decoding; got: %v", err)
	}

	agg, found := aggs.Filters("messages")
	if !found {
		t.Fatalf("expected aggregation to be found; got: %v", found)
	}
	if agg == nil {
		t.Fatalf("expected aggregation != nil; got: %v", agg)
	}
	if agg.Buckets == nil {
		t.Fatalf("expected aggregation buckets != %v; got: %v", nil, agg.Buckets)
	}
	if len(agg.Buckets) != 2 {
		t.Fatalf("expected %d buckets; got: %d", 2, len(agg.Buckets))
	}

	if agg.Buckets[0].DocCount != 34 {
		t.Fatalf("expected DocCount = %d; got: %d", 34, agg.Buckets[0].DocCount)
	}
	subAgg, found := agg.Buckets[0].Histogram("monthly")
	if !found {
		t.Fatalf("expected sub aggregation to be found; got: %v", found)
	}
	if subAgg == nil {
		t.Fatalf("expected sub aggregation != %v; got: %v", nil, subAgg)
	}

	if agg.Buckets[1].DocCount != 439 {
		t.Fatalf("expected DocCount = %d; got: %d", 439, agg.Buckets[1].DocCount)
	}
	subAgg, found = agg.Buckets[1].Histogram("monthly")
	if !found {
		t.Fatalf("expected sub aggregation to be found; got: %v", found)
	}
	if subAgg == nil {
		t.Fatalf("expected sub aggregation != %v; got: %v", nil, subAgg)
	}
}

func TestAggsBucketFiltersWithNamedBuckets(t *testing.T) {
	s := `{
  "messages" : {
    "buckets" : {
      "errors" : {
        "doc_count" : 34,
        "monthly" : {
          "buckets" : []
        }
      },
      "warnings" : {
        "doc_count" : 439,
        "monthly" : {
          "buckets" : []
        }
      }
    }
  }
}`

	aggs := new(Aggregations)
	err := json.Unmarshal([]byte(s), &aggs)
	if err != nil {
		t.Fatalf("expected no error decoding; got: %v", err)
	}

	agg, found := aggs.Filters("messages")
	if !found {
		t.Fatalf("expected aggregation to be found; got: %v", found)
	}
	if agg == nil {
		t.Fatalf("expected aggregation != nil; got: %v", agg)
	}
	if agg.NamedBuckets == nil {
		t.Fatalf("expected aggregation buckets != %v; got: %v", nil, agg.NamedBuckets)
	}
	if len(agg.NamedBuckets) != 2 {
		t.Fatalf("expected %d buckets; got: %d", 2, len(agg.NamedBuckets))
	}

	if agg.NamedBuckets["errors"].DocCount != 34 {
		t.Fatalf("expected DocCount = %d; got: %d", 34, agg.NamedBuckets["errors"].DocCount)
	}
	subAgg, found := agg.NamedBuckets["errors"].Histogram("monthly")
	if !found {
		t.Fatalf("expected sub aggregation to be found; got: %v", found)
	}
	if subAgg == nil {
		t.Fatalf("expected sub aggregation != %v; got: %v", nil, subAgg)
	}

	if agg.NamedBuckets["warnings"].DocCount != 439 {
		t.Fatalf("expected DocCount = %d; got: %d", 439, agg.NamedBuckets["warnings"].DocCount)
	}
	subAgg, found = agg.NamedBuckets["warnings"].Histogram("monthly")
	if !found {
		t.Fatalf("expected sub aggregation to be found; got: %v", found)
	}
	if subAgg == nil {
		t.Fatalf("expected sub aggregation != %v; got: %v", nil, subAgg)
	}
}

func TestAggsBucketMissing(t *testing.T) {
	s := `{
	"products_without_a_price" : {
		"doc_count" : 10
	}
}`

	aggs := new(Aggregations)
	err := json.Unmarshal([]byte(s), &aggs)
	if err != nil {
		t.Fatalf("expected no error decoding; got: %v", err)
	}

	agg, found := aggs.Missing("products_without_a_price")
	if !found {
		t.Fatalf("expected aggregation to be found; got: %v", found)
	}
	if agg == nil {
		t.Fatalf("expected aggregation != nil; got: %v", agg)
	}
	if agg.DocCount != 10 {
		t.Fatalf("expected aggregation DocCount = %d; got: %d", 10, agg.DocCount)
	}
}

func TestAggsBucketNested(t *testing.T) {
	s := `{
	"resellers": {
		"min_price": {
			"value" : 350
		}
	}
}`

	aggs := new(Aggregations)
	err := json.Unmarshal([]byte(s), &aggs)
	if err != nil {
		t.Fatalf("expected no error decoding; got: %v", err)
	}

	agg, found := aggs.Nested("resellers")
	if !found {
		t.Fatalf("expected aggregation to be found; got: %v", found)
	}
	if agg == nil {
		t.Fatalf("expected aggregation != nil; got: %v", agg)
	}
	if agg.DocCount != 0 {
		t.Fatalf("expected aggregation DocCount = %d; got: %d", 0, agg.DocCount)
	}

	// Sub-aggregation
	subAgg, found := agg.Avg("min_price")
	if !found {
		t.Fatalf("expected sub-aggregation to be found; got: %v", found)
	}
	if subAgg == nil {
		t.Fatalf("expected sub-aggregation != nil; got: %v", subAgg)
	}
	if subAgg.Value == nil {
		t.Fatalf("expected sub-aggregation value != nil; got: %v", subAgg.Value)
	}
	if *subAgg.Value != float64(350) {
		t.Fatalf("expected sub-aggregation value = %v; got: %v", float64(350), *subAgg.Value)
	}
}

func TestAggsBucketReverseNested(t *testing.T) {
	s := `{
	"comment_to_issue": {
		"doc_count" : 10
	}
}`

	aggs := new(Aggregations)
	err := json.Unmarshal([]byte(s), &aggs)
	if err != nil {
		t.Fatalf("expected no error decoding; got: %v", err)
	}

	agg, found := aggs.ReverseNested("comment_to_issue")
	if !found {
		t.Fatalf("expected aggregation to be found; got: %v", found)
	}
	if agg == nil {
		t.Fatalf("expected aggregation != nil; got: %v", agg)
	}
	if agg.DocCount != 10 {
		t.Fatalf("expected aggregation DocCount = %d; got: %d", 10, agg.DocCount)
	}
}

func TestAggsBucketChildren(t *testing.T) {
	s := `{
	"to-answers": {
		"doc_count" : 10
	}
}`

	aggs := new(Aggregations)
	err := json.Unmarshal([]byte(s), &aggs)
	if err != nil {
		t.Fatalf("expected no error decoding; got: %v", err)
	}

	agg, found := aggs.Children("to-answers")
	if !found {
		t.Fatalf("expected aggregation to be found; got: %v", found)
	}
	if agg == nil {
		t.Fatalf("expected aggregation != nil; got: %v", agg)
	}
	if agg.DocCount != 10 {
		t.Fatalf("expected aggregation DocCount = %d; got: %d", 10, agg.DocCount)
	}
}

func TestAggsBucketTerms(t *testing.T) {
	s := `{
	"users" : {
	  "doc_count_error_upper_bound" : 1,
	  "sum_other_doc_count" : 2,
	  "buckets" : [ {
	    "key" : "olivere",
	    "doc_count" : 2
	  }, {
	    "key" : "sandrae",
	    "doc_count" : 1
	  } ]
	}
}`

	aggs := new(Aggregations)
	err := json.Unmarshal([]byte(s), &aggs)
	if err != nil {
		t.Fatalf("expected no error decoding; got: %v", err)
	}

	agg, found := aggs.Terms("users")
	if !found {
		t.Fatalf("expected aggregation to be found; got: %v", found)
	}
	if agg == nil {
		t.Fatalf("expected aggregation != nil; got: %v", agg)
	}
	if agg.Buckets == nil {
		t.Fatalf("expected aggregation buckets != nil; got: %v", agg.Buckets)
	}
	if len(agg.Buckets) != 2 {
		t.Errorf("expected %d bucket entries; got: %d", 2, len(agg.Buckets))
	}
	if agg.Buckets[0].Key != "olivere" {
		t.Errorf("expected key %q; got: %q", "olivere", agg.Buckets[0].Key)
	}
	if agg.Buckets[0].DocCount != 2 {
		t.Errorf("expected doc count %d; got: %d", 2, agg.Buckets[0].DocCount)
	}
	if agg.Buckets[1].Key != "sandrae" {
		t.Errorf("expected key %q; got: %q", "sandrae", agg.Buckets[1].Key)
	}
	if agg.Buckets[1].DocCount != 1 {
		t.Errorf("expected doc count %d; got: %d", 1, agg.Buckets[1].DocCount)
	}
}

func TestAggsBucketTermsWithNumericKeys(t *testing.T) {
	s := `{
	"users" : {
	  "doc_count_error_upper_bound" : 1,
	  "sum_other_doc_count" : 2,
	  "buckets" : [ {
	    "key" : 17,
	    "doc_count" : 2
	  }, {
	    "key" : 21,
	    "doc_count" : 1
	  } ]
	}
}`

	aggs := new(Aggregations)
	err := json.Unmarshal([]byte(s), &aggs)
	if err != nil {
		t.Fatalf("expected no error decoding; got: %v", err)
	}

	agg, found := aggs.Terms("users")
	if !found {
		t.Fatalf("expected aggregation to be found; got: %v", found)
	}
	if agg == nil {
		t.Fatalf("expected aggregation != nil; got: %v", agg)
	}
	if agg.Buckets == nil {
		t.Fatalf("expected aggregation buckets != nil; got: %v", agg.Buckets)
	}
	if len(agg.Buckets) != 2 {
		t.Errorf("expected %d bucket entries; got: %d", 2, len(agg.Buckets))
	}
	if agg.Buckets[0].Key != float64(17) {
		t.Errorf("expected key %v; got: %v", 17, agg.Buckets[0].Key)
	}
	if got, err := agg.Buckets[0].KeyNumber.Int64(); err != nil {
		t.Errorf("expected to convert key to int64; got: %v", err)
	} else if got != 17 {
		t.Errorf("expected key %v; got: %v", 17, agg.Buckets[0].Key)
	}
	if agg.Buckets[0].DocCount != 2 {
		t.Errorf("expected doc count %d; got: %d", 2, agg.Buckets[0].DocCount)
	}
	if agg.Buckets[1].Key != float64(21) {
		t.Errorf("expected key %v; got: %v", 21, agg.Buckets[1].Key)
	}
	if got, err := agg.Buckets[1].KeyNumber.Int64(); err != nil {
		t.Errorf("expected to convert key to int64; got: %v", err)
	} else if got != 21 {
		t.Errorf("expected key %v; got: %v", 21, agg.Buckets[1].Key)
	}
	if agg.Buckets[1].DocCount != 1 {
		t.Errorf("expected doc count %d; got: %d", 1, agg.Buckets[1].DocCount)
	}
}

func TestAggsBucketTermsWithBoolKeys(t *testing.T) {
	s := `{
	"users" : {
	  "doc_count_error_upper_bound" : 1,
	  "sum_other_doc_count" : 2,
	  "buckets" : [ {
	    "key" : true,
	    "doc_count" : 2
	  }, {
	    "key" : false,
	    "doc_count" : 1
	  } ]
	}
}`

	aggs := new(Aggregations)
	err := json.Unmarshal([]byte(s), &aggs)
	if err != nil {
		t.Fatalf("expected no error decoding; got: %v", err)
	}

	agg, found := aggs.Terms("users")
	if !found {
		t.Fatalf("expected aggregation to be found; got: %v", found)
	}
	if agg == nil {
		t.Fatalf("expected aggregation != nil; got: %v", agg)
	}
	if agg.Buckets == nil {
		t.Fatalf("expected aggregation buckets != nil; got: %v", agg.Buckets)
	}
	if len(agg.Buckets) != 2 {
		t.Errorf("expected %d bucket entries; got: %d", 2, len(agg.Buckets))
	}
	if agg.Buckets[0].Key != true {
		t.Errorf("expected key %v; got: %v", true, agg.Buckets[0].Key)
	}
	if agg.Buckets[0].DocCount != 2 {
		t.Errorf("expected doc count %d; got: %d", 2, agg.Buckets[0].DocCount)
	}
	if agg.Buckets[1].Key != false {
		t.Errorf("expected key %v; got: %v", false, agg.Buckets[1].Key)
	}
	if agg.Buckets[1].DocCount != 1 {
		t.Errorf("expected doc count %d; got: %d", 1, agg.Buckets[1].DocCount)
	}
}

func TestAggsBucketSignificantTerms(t *testing.T) {
	s := `{
	"significantCrimeTypes" : {
    "doc_count": 47347,
    "buckets" : [
      {
        "key": "Bicycle theft",
        "doc_count": 3640,
        "score": 0.371235374214817,
        "bg_count": 66799
      }
    ]
	}
}`

	aggs := new(Aggregations)
	err := json.Unmarshal([]byte(s), &aggs)
	if err != nil {
		t.Fatalf("expected no error decoding; got: %v", err)
	}

	agg, found := aggs.SignificantTerms("significantCrimeTypes")
	if !found {
		t.Fatalf("expected aggregation to be found; got: %v", found)
	}
	if agg == nil {
		t.Fatalf("expected aggregation != nil; got: %v", agg)
	}
	if agg.DocCount != 47347 {
		t.Fatalf("expected aggregation DocCount != %d; got: %d", 47347, agg.DocCount)
	}
	if agg.Buckets == nil {
		t.Fatalf("expected aggregation buckets != nil; got: %v", agg.Buckets)
	}
	if len(agg.Buckets) != 1 {
		t.Errorf("expected %d bucket entries; got: %d", 1, len(agg.Buckets))
	}
	if agg.Buckets[0].Key != "Bicycle theft" {
		t.Errorf("expected key = %q; got: %q", "Bicycle theft", agg.Buckets[0].Key)
	}
	if agg.Buckets[0].DocCount != 3640 {
		t.Errorf("expected doc count = %d; got: %d", 3640, agg.Buckets[0].DocCount)
	}
	if agg.Buckets[0].Score != float64(0.371235374214817) {
		t.Errorf("expected score = %v; got: %v", float64(0.371235374214817), agg.Buckets[0].Score)
	}
	if agg.Buckets[0].BgCount != 66799 {
		t.Errorf("expected BgCount = %d; got: %d", 66799, agg.Buckets[0].BgCount)
	}
}

func TestAggsBucketSampler(t *testing.T) {
	s := `{
	"sample" : {
    "doc_count": 1000,
    "keywords": {
    	"doc_count": 1000,
	    "buckets" : [
	      {
	        "key": "bend",
	        "doc_count": 58,
	        "score": 37.982536582524276,
	        "bg_count": 103
	      }
	    ]
    }
	}
}`

	aggs := new(Aggregations)
	err := json.Unmarshal([]byte(s), &aggs)
	if err != nil {
		t.Fatalf("expected no error decoding; got: %v", err)
	}

	agg, found := aggs.Sampler("sample")
	if !found {
		t.Fatalf("expected aggregation to be found; got: %v", found)
	}
	if agg == nil {
		t.Fatalf("expected aggregation != nil; got: %v", agg)
	}
	if agg.DocCount != 1000 {
		t.Fatalf("expected aggregation DocCount != %d; got: %d", 1000, agg.DocCount)
	}
	sub, found := agg.Aggregations["keywords"]
	if !found {
		t.Fatalf("expected sub aggregation %q", "keywords")
	}
	if sub == nil {
		t.Fatalf("expected sub aggregation %q; got: %v", "keywords", sub)
	}
}

func TestAggsBucketRange(t *testing.T) {
	s := `{
	"price_ranges" : {
		"buckets": [
			{
				"to": 50,
				"doc_count": 2
			},
			{
				"from": 50,
				"to": 100,
				"doc_count": 4
			},
			{
				"from": 100,
				"doc_count": 4
			}
		]
	}
}`

	aggs := new(Aggregations)
	err := json.Unmarshal([]byte(s), &aggs)
	if err != nil {
		t.Fatalf("expected no error decoding; got: %v", err)
	}

	agg, found := aggs.Range("price_ranges")
	if !found {
		t.Fatalf("expected aggregation to be found; got: %v", found)
	}
	if agg == nil {
		t.Fatalf("expected aggregation != nil; got: %v", agg)
	}
	if agg.Buckets == nil {
		t.Fatalf("expected aggregation buckets != nil; got: %v", agg.Buckets)
	}
	if len(agg.Buckets) != 3 {
		t.Errorf("expected %d bucket entries; got: %d", 3, len(agg.Buckets))
	}
	if agg.Buckets[0].From != nil {
		t.Errorf("expected From = %v; got: %v", nil, agg.Buckets[0].From)
	}
	if agg.Buckets[0].To == nil {
		t.Errorf("expected To != %v; got: %v", nil, agg.Buckets[0].To)
	}
	if *agg.Buckets[0].To != float64(50) {
		t.Errorf("expected To = %v; got: %v", float64(50), *agg.Buckets[0].To)
	}
	if agg.Buckets[0].DocCount != 2 {
		t.Errorf("expected DocCount = %d; got: %d", 2, agg.Buckets[0].DocCount)
	}
	if agg.Buckets[1].From == nil {
		t.Errorf("expected From != %v; got: %v", nil, agg.Buckets[1].From)
	}
	if *agg.Buckets[1].From != float64(50) {
		t.Errorf("expected From = %v; got: %v", float64(50), *agg.Buckets[1].From)
	}
	if agg.Buckets[1].To == nil {
		t.Errorf("expected To != %v; got: %v", nil, agg.Buckets[1].To)
	}
	if *agg.Buckets[1].To != float64(100) {
		t.Errorf("expected To = %v; got: %v", float64(100), *agg.Buckets[1].To)
	}
	if agg.Buckets[1].DocCount != 4 {
		t.Errorf("expected DocCount = %d; got: %d", 4, agg.Buckets[1].DocCount)
	}
	if agg.Buckets[2].From == nil {
		t.Errorf("expected From != %v; got: %v", nil, agg.Buckets[2].From)
	}
	if *agg.Buckets[2].From != float64(100) {
		t.Errorf("expected From = %v; got: %v", float64(100), *agg.Buckets[2].From)
	}
	if agg.Buckets[2].To != nil {
		t.Errorf("expected To = %v; got: %v", nil, agg.Buckets[2].To)
	}
	if agg.Buckets[2].DocCount != 4 {
		t.Errorf("expected DocCount = %d; got: %d", 4, agg.Buckets[2].DocCount)
	}
}

func TestAggsBucketDateRange(t *testing.T) {
	s := `{
	"range": {
		"buckets": [
			{
				"to": 1.3437792E+12,
				"to_as_string": "08-2012",
				"doc_count": 7
			},
			{
				"from": 1.3437792E+12,
				"from_as_string": "08-2012",
				"doc_count": 2
			}
		]
	}
}`

	aggs := new(Aggregations)
	err := json.Unmarshal([]byte(s), &aggs)
	if err != nil {
		t.Fatalf("expected no error decoding; got: %v", err)
	}

	agg, found := aggs.DateRange("range")
	if !found {
		t.Fatalf("expected aggregation to be found; got: %v", found)
	}
	if agg == nil {
		t.Fatalf("expected aggregation != nil; got: %v", agg)
	}
	if agg.Buckets == nil {
		t.Fatalf("expected aggregation buckets != nil; got: %v", agg.Buckets)
	}
	if len(agg.Buckets) != 2 {
		t.Errorf("expected %d bucket entries; got: %d", 2, len(agg.Buckets))
	}
	if agg.Buckets[0].From != nil {
		t.Errorf("expected From = %v; got: %v", nil, agg.Buckets[0].From)
	}
	if agg.Buckets[0].To == nil {
		t.Errorf("expected To != %v; got: %v", nil, agg.Buckets[0].To)
	}
	if *agg.Buckets[0].To != float64(1.3437792E+12) {
		t.Errorf("expected To = %v; got: %v", float64(1.3437792E+12), *agg.Buckets[0].To)
	}
	if agg.Buckets[0].ToAsString != "08-2012" {
		t.Errorf("expected ToAsString = %q; got: %q", "08-2012", agg.Buckets[0].ToAsString)
	}
	if agg.Buckets[0].DocCount != 7 {
		t.Errorf("expected DocCount = %d; got: %d", 7, agg.Buckets[0].DocCount)
	}
	if agg.Buckets[1].From == nil {
		t.Errorf("expected From != %v; got: %v", nil, agg.Buckets[1].From)
	}
	if *agg.Buckets[1].From != float64(1.3437792E+12) {
		t.Errorf("expected From = %v; got: %v", float64(1.3437792E+12), *agg.Buckets[1].From)
	}
	if agg.Buckets[1].FromAsString != "08-2012" {
		t.Errorf("expected FromAsString = %q; got: %q", "08-2012", agg.Buckets[1].FromAsString)
	}
	if agg.Buckets[1].To != nil {
		t.Errorf("expected To = %v; got: %v", nil, agg.Buckets[1].To)
	}
	if agg.Buckets[1].DocCount != 2 {
		t.Errorf("expected DocCount = %d; got: %d", 2, agg.Buckets[1].DocCount)
	}
}

func TestAggsBucketIPv4Range(t *testing.T) {
	s := `{
	"ip_ranges": {
		"buckets" : [
			{
				"to": 167772165,
				"to_as_string": "10.0.0.5",
				"doc_count": 4
			},
			{
				"from": 167772165,
				"from_as_string": "10.0.0.5",
				"doc_count": 6
			}
		]
	}
}`

	aggs := new(Aggregations)
	err := json.Unmarshal([]byte(s), &aggs)
	if err != nil {
		t.Fatalf("expected no error decoding; got: %v", err)
	}

	agg, found := aggs.IPv4Range("ip_ranges")
	if !found {
		t.Fatalf("expected aggregation to be found; got: %v", found)
	}
	if agg == nil {
		t.Fatalf("expected aggregation != nil; got: %v", agg)
	}
	if agg.Buckets == nil {
		t.Fatalf("expected aggregation buckets != nil; got: %v", agg.Buckets)
	}
	if len(agg.Buckets) != 2 {
		t.Errorf("expected %d bucket entries; got: %d", 2, len(agg.Buckets))
	}
	if agg.Buckets[0].From != nil {
		t.Errorf("expected From = %v; got: %v", nil, agg.Buckets[0].From)
	}
	if agg.Buckets[0].To == nil {
		t.Errorf("expected To != %v; got: %v", nil, agg.Buckets[0].To)
	}
	if *agg.Buckets[0].To != float64(167772165) {
		t.Errorf("expected To = %v; got: %v", float64(167772165), *agg.Buckets[0].To)
	}
	if agg.Buckets[0].ToAsString != "10.0.0.5" {
		t.Errorf("expected ToAsString = %q; got: %q", "10.0.0.5", agg.Buckets[0].ToAsString)
	}
	if agg.Buckets[0].DocCount != 4 {
		t.Errorf("expected DocCount = %d; got: %d", 4, agg.Buckets[0].DocCount)
	}
	if agg.Buckets[1].From == nil {
		t.Errorf("expected From != %v; got: %v", nil, agg.Buckets[1].From)
	}
	if *agg.Buckets[1].From != float64(167772165) {
		t.Errorf("expected From = %v; got: %v", float64(167772165), *agg.Buckets[1].From)
	}
	if agg.Buckets[1].FromAsString != "10.0.0.5" {
		t.Errorf("expected FromAsString = %q; got: %q", "10.0.0.5", agg.Buckets[1].FromAsString)
	}
	if agg.Buckets[1].To != nil {
		t.Errorf("expected To = %v; got: %v", nil, agg.Buckets[1].To)
	}
	if agg.Buckets[1].DocCount != 6 {
		t.Errorf("expected DocCount = %d; got: %d", 6, agg.Buckets[1].DocCount)
	}
}

func TestAggsBucketHistogram(t *testing.T) {
	s := `{
	"prices" : {
		"buckets": [
			{
				"key": 0,
				"doc_count": 2
			},
			{
				"key": 50,
				"doc_count": 4
			},
			{
				"key": 150,
				"doc_count": 3
			}
		]
	}
}`

	aggs := new(Aggregations)
	err := json.Unmarshal([]byte(s), &aggs)
	if err != nil {
		t.Fatalf("expected no error decoding; got: %v", err)
	}

	agg, found := aggs.Histogram("prices")
	if !found {
		t.Fatalf("expected aggregation to be found; got: %v", found)
	}
	if agg == nil {
		t.Fatalf("expected aggregation != nil; got: %v", agg)
	}
	if agg.Buckets == nil {
		t.Fatalf("expected aggregation buckets != nil; got: %v", agg.Buckets)
	}
	if len(agg.Buckets) != 3 {
		t.Errorf("expected %d buckets; got: %d", 3, len(agg.Buckets))
	}
	if agg.Buckets[0].Key != 0 {
		t.Errorf("expected key = %v; got: %v", 0, agg.Buckets[0].Key)
	}
	if agg.Buckets[0].KeyAsString != nil {
		t.Fatalf("expected key_as_string = %v; got: %q", nil, *agg.Buckets[0].KeyAsString)
	}
	if agg.Buckets[0].DocCount != 2 {
		t.Errorf("expected doc count = %d; got: %d", 2, agg.Buckets[0].DocCount)
	}
	if agg.Buckets[1].Key != 50 {
		t.Errorf("expected key = %v; got: %v", 50, agg.Buckets[1].Key)
	}
	if agg.Buckets[1].KeyAsString != nil {
		t.Fatalf("expected key_as_string = %v; got: %q", nil, *agg.Buckets[1].KeyAsString)
	}
	if agg.Buckets[1].DocCount != 4 {
		t.Errorf("expected doc count = %d; got: %d", 4, agg.Buckets[1].DocCount)
	}
	if agg.Buckets[2].Key != 150 {
		t.Errorf("expected key = %v; got: %v", 150, agg.Buckets[2].Key)
	}
	if agg.Buckets[2].KeyAsString != nil {
		t.Fatalf("expected key_as_string = %v; got: %q", nil, *agg.Buckets[2].KeyAsString)
	}
	if agg.Buckets[2].DocCount != 3 {
		t.Errorf("expected doc count = %d; got: %d", 3, agg.Buckets[2].DocCount)
	}
}

func TestAggsBucketDateHistogram(t *testing.T) {
	s := `{
	"articles_over_time": {
	  "buckets": [
	      {
	          "key_as_string": "2013-02-02",
	          "key": 1328140800000,
	          "doc_count": 1
	      },
	      {
	          "key_as_string": "2013-03-02",
	          "key": 1330646400000,
	          "doc_count": 2
	      }
	  ]
	}
}`

	aggs := new(Aggregations)
	err := json.Unmarshal([]byte(s), &aggs)
	if err != nil {
		t.Fatalf("expected no error decoding; got: %v", err)
	}

	agg, found := aggs.DateHistogram("articles_over_time")
	if !found {
		t.Fatalf("expected aggregation to be found; got: %v", found)
	}
	if agg == nil {
		t.Fatalf("expected aggregation != nil; got: %v", agg)
	}
	if agg.Buckets == nil {
		t.Fatalf("expected aggregation buckets != nil; got: %v", agg.Buckets)
	}
	if len(agg.Buckets) != 2 {
		t.Errorf("expected %d bucket entries; got: %d", 2, len(agg.Buckets))
	}
	if agg.Buckets[0].Key != 1328140800000 {
		t.Errorf("expected key %v; got: %v", 1328140800000, agg.Buckets[0].Key)
	}
	if agg.Buckets[0].KeyAsString == nil {
		t.Fatalf("expected key_as_string != nil; got: %v", agg.Buckets[0].KeyAsString)
	}
	if *agg.Buckets[0].KeyAsString != "2013-02-02" {
		t.Errorf("expected key_as_string %q; got: %q", "2013-02-02", *agg.Buckets[0].KeyAsString)
	}
	if agg.Buckets[0].DocCount != 1 {
		t.Errorf("expected doc count %d; got: %d", 1, agg.Buckets[0].DocCount)
	}
	if agg.Buckets[1].Key != 1330646400000 {
		t.Errorf("expected key %v; got: %v", 1330646400000, agg.Buckets[1].Key)
	}
	if agg.Buckets[1].KeyAsString == nil {
		t.Fatalf("expected key_as_string != nil; got: %v", agg.Buckets[1].KeyAsString)
	}
	if *agg.Buckets[1].KeyAsString != "2013-03-02" {
		t.Errorf("expected key_as_string %q; got: %q", "2013-03-02", *agg.Buckets[1].KeyAsString)
	}
	if agg.Buckets[1].DocCount != 2 {
		t.Errorf("expected doc count %d; got: %d", 2, agg.Buckets[1].DocCount)
	}
}

func TestAggsMetricsGeoBounds(t *testing.T) {
	s := `{
  "viewport": {
    "bounds": {
      "top_left": {
        "lat": 80.45,
        "lon": -160.22
      },
      "bottom_right": {
        "lat": 40.65,
        "lon": 42.57
      }
    }
  }
}`

	aggs := new(Aggregations)
	err := json.Unmarshal([]byte(s), &aggs)
	if err != nil {
		t.Fatalf("expected no error decoding; got: %v", err)
	}

	agg, found := aggs.GeoBounds("viewport")
	if !found {
		t.Fatalf("expected aggregation to be found; got: %v", found)
	}
	if agg == nil {
		t.Fatalf("expected aggregation != nil; got: %v", agg)
	}
	if agg.Bounds.TopLeft.Latitude != float64(80.45) {
		t.Fatalf("expected Bounds.TopLeft.Latitude != %v; got: %v", float64(80.45), agg.Bounds.TopLeft.Latitude)
	}
	if agg.Bounds.TopLeft.Longitude != float64(-160.22) {
		t.Fatalf("expected Bounds.TopLeft.Longitude != %v; got: %v", float64(-160.22), agg.Bounds.TopLeft.Longitude)
	}
	if agg.Bounds.BottomRight.Latitude != float64(40.65) {
		t.Fatalf("expected Bounds.BottomRight.Latitude != %v; got: %v", float64(40.65), agg.Bounds.BottomRight.Latitude)
	}
	if agg.Bounds.BottomRight.Longitude != float64(42.57) {
		t.Fatalf("expected Bounds.BottomRight.Longitude != %v; got: %v", float64(42.57), agg.Bounds.BottomRight.Longitude)
	}
}

func TestAggsBucketGeoHash(t *testing.T) {
	s := `{
	"myLarge-GrainGeoHashGrid": {
		"buckets": [
			{
				"key": "svz",
				"doc_count": 10964
			},
			{
				"key": "sv8",
				"doc_count": 3198
			}
		]
	}
}`

	aggs := new(Aggregations)
	err := json.Unmarshal([]byte(s), &aggs)
	if err != nil {
		t.Fatalf("expected no error decoding; got: %v", err)
	}

	agg, found := aggs.GeoHash("myLarge-GrainGeoHashGrid")
	if !found {
		t.Fatalf("expected aggregation to be found; got: %v", found)
	}
	if agg == nil {
		t.Fatalf("expected aggregation != nil; got: %v", agg)
	}
	if agg.Buckets == nil {
		t.Fatalf("expected aggregation buckets != nil; got: %v", agg.Buckets)
	}
	if len(agg.Buckets) != 2 {
		t.Errorf("expected %d bucket entries; got: %d", 2, len(agg.Buckets))
	}
	if agg.Buckets[0].Key != "svz" {
		t.Errorf("expected key %q; got: %q", "svz", agg.Buckets[0].Key)
	}
	if agg.Buckets[0].DocCount != 10964 {
		t.Errorf("expected doc count %d; got: %d", 10964, agg.Buckets[0].DocCount)
	}
	if agg.Buckets[1].Key != "sv8" {
		t.Errorf("expected key %q; got: %q", "sv8", agg.Buckets[1].Key)
	}
	if agg.Buckets[1].DocCount != 3198 {
		t.Errorf("expected doc count %d; got: %d", 3198, agg.Buckets[1].DocCount)
	}
}

func TestAggsBucketGeoDistance(t *testing.T) {
	s := `{
	"rings" : {
		"buckets": [
			{
				"unit": "km",
				"to": 100.0,
				"doc_count": 3
			},
			{
				"unit": "km",
				"from": 100.0,
				"to": 300.0,
				"doc_count": 1
			},
			{
				"unit": "km",
				"from": 300.0,
				"doc_count": 7
			}
		]
	}
}`

	aggs := new(Aggregations)
	err := json.Unmarshal([]byte(s), &aggs)
	if err != nil {
		t.Fatalf("expected no error decoding; got: %v", err)
	}

	agg, found := aggs.GeoDistance("rings")
	if !found {
		t.Fatalf("expected aggregation to be found; got: %v", found)
	}
	if agg == nil {
		t.Fatalf("expected aggregation != nil; got: %v", agg)
	}
	if agg.Buckets == nil {
		t.Fatalf("expected aggregation buckets != nil; got: %v", agg.Buckets)
	}
	if len(agg.Buckets) != 3 {
		t.Errorf("expected %d bucket entries; got: %d", 3, len(agg.Buckets))
	}
	if agg.Buckets[0].From != nil {
		t.Errorf("expected From = %v; got: %v", nil, agg.Buckets[0].From)
	}
	if agg.Buckets[0].To == nil {
		t.Errorf("expected To != %v; got: %v", nil, agg.Buckets[0].To)
	}
	if *agg.Buckets[0].To != float64(100.0) {
		t.Errorf("expected To = %v; got: %v", float64(100.0), *agg.Buckets[0].To)
	}
	if agg.Buckets[0].DocCount != 3 {
		t.Errorf("expected DocCount = %d; got: %d", 4, agg.Buckets[0].DocCount)
	}

	if agg.Buckets[1].From == nil {
		t.Errorf("expected From != %v; got: %v", nil, agg.Buckets[1].From)
	}
	if *agg.Buckets[1].From != float64(100.0) {
		t.Errorf("expected From = %v; got: %v", float64(100.0), *agg.Buckets[1].From)
	}
	if agg.Buckets[1].To == nil {
		t.Errorf("expected To != %v; got: %v", nil, agg.Buckets[1].To)
	}
	if *agg.Buckets[1].To != float64(300.0) {
		t.Errorf("expected From = %v; got: %v", float64(300.0), *agg.Buckets[1].To)
	}
	if agg.Buckets[1].DocCount != 1 {
		t.Errorf("expected DocCount = %d; got: %d", 1, agg.Buckets[1].DocCount)
	}

	if agg.Buckets[2].From == nil {
		t.Errorf("expected From != %v; got: %v", nil, agg.Buckets[2].From)
	}
	if *agg.Buckets[2].From != float64(300.0) {
		t.Errorf("expected From = %v; got: %v", float64(300.0), *agg.Buckets[2].From)
	}
	if agg.Buckets[2].To != nil {
		t.Errorf("expected To = %v; got: %v", nil, agg.Buckets[2].To)
	}
	if agg.Buckets[2].DocCount != 7 {
		t.Errorf("expected DocCount = %d; got: %d", 7, agg.Buckets[2].DocCount)
	}
}

func TestAggsSubAggregates(t *testing.T) {
	rs := `{
	"users" : {
	  "doc_count_error_upper_bound" : 1,
	  "sum_other_doc_count" : 2,
	  "buckets" : [ {
	    "key" : "olivere",
	    "doc_count" : 2,
	    "ts" : {
	      "buckets" : [ {
	        "key_as_string" : "2012-01-01T00:00:00.000Z",
	        "key" : 1325376000000,
	        "doc_count" : 2
	      } ]
	    }
	  }, {
	    "key" : "sandrae",
	    "doc_count" : 1,
	    "ts" : {
	      "buckets" : [ {
	        "key_as_string" : "2011-01-01T00:00:00.000Z",
	        "key" : 1293840000000,
	        "doc_count" : 1
	      } ]
	    }
	  } ]
	}
}`

	aggs := new(Aggregations)
	err := json.Unmarshal([]byte(rs), &aggs)
	if err != nil {
		t.Fatalf("expected no error decoding; got: %v", err)
	}

	// Access top-level aggregation
	users, found := aggs.Terms("users")
	if !found {
		t.Fatalf("expected users aggregation to be found; got: %v", found)
	}
	if users == nil {
		t.Fatalf("expected users aggregation; got: %v", users)
	}
	if users.Buckets == nil {
		t.Fatalf("expected users buckets; got: %v", users.Buckets)
	}
	if len(users.Buckets) != 2 {
		t.Errorf("expected %d bucket entries; got: %d", 2, len(users.Buckets))
	}
	if users.Buckets[0].Key != "olivere" {
		t.Errorf("expected key %q; got: %q", "olivere", users.Buckets[0].Key)
	}
	if users.Buckets[0].DocCount != 2 {
		t.Errorf("expected doc count %d; got: %d", 2, users.Buckets[0].DocCount)
	}
	if users.Buckets[1].Key != "sandrae" {
		t.Errorf("expected key %q; got: %q", "sandrae", users.Buckets[1].Key)
	}
	if users.Buckets[1].DocCount != 1 {
		t.Errorf("expected doc count %d; got: %d", 1, users.Buckets[1].DocCount)
	}

	// Access sub-aggregation
	ts, found := users.Buckets[0].DateHistogram("ts")
	if !found {
		t.Fatalf("expected ts aggregation to be found; got: %v", found)
	}
	if ts == nil {
		t.Fatalf("expected ts aggregation; got: %v", ts)
	}
	if ts.Buckets == nil {
		t.Fatalf("expected ts buckets; got: %v", ts.Buckets)
	}
	if len(ts.Buckets) != 1 {
		t.Errorf("expected %d bucket entries; got: %d", 1, len(ts.Buckets))
	}
	if ts.Buckets[0].Key != 1325376000000 {
		t.Errorf("expected key %v; got: %v", 1325376000000, ts.Buckets[0].Key)
	}
	if ts.Buckets[0].KeyAsString == nil {
		t.Fatalf("expected key_as_string != %v; got: %v", nil, ts.Buckets[0].KeyAsString)
	}
	if *ts.Buckets[0].KeyAsString != "2012-01-01T00:00:00.000Z" {
		t.Errorf("expected key_as_string %q; got: %q", "2012-01-01T00:00:00.000Z", *ts.Buckets[0].KeyAsString)
	}
}

func TestAggsPipelineAvgBucket(t *testing.T) {
	s := `{
	"avg_monthly_sales" : {
	  "value" : 328.33333333333333
  }
}`

	aggs := new(Aggregations)
	err := json.Unmarshal([]byte(s), &aggs)
	if err != nil {
		t.Fatalf("expected no error decoding; got: %v", err)
	}

	agg, found := aggs.AvgBucket("avg_monthly_sales")
	if !found {
		t.Fatalf("expected aggregation to be found; got: %v", found)
	}
	if agg == nil {
		t.Fatalf("expected aggregation != nil; got: %v", agg)
	}
	if agg.Value == nil {
		t.Fatalf("expected aggregation value != nil; got: %v", agg.Value)
	}
	if *agg.Value != float64(328.33333333333333) {
		t.Fatalf("expected aggregation value = %v; got: %v", float64(328.33333333333333), *agg.Value)
	}
}

func TestAggsPipelineSumBucket(t *testing.T) {
	s := `{
	"sum_monthly_sales" : {
	  "value" : 985
  }
}`

	aggs := new(Aggregations)
	err := json.Unmarshal([]byte(s), &aggs)
	if err != nil {
		t.Fatalf("expected no error decoding; got: %v", err)
	}

	agg, found := aggs.SumBucket("sum_monthly_sales")
	if !found {
		t.Fatalf("expected aggregation to be found; got: %v", found)
	}
	if agg == nil {
		t.Fatalf("expected aggregation != nil; got: %v", agg)
	}
	if agg.Value == nil {
		t.Fatalf("expected aggregation value != nil; got: %v", agg.Value)
	}
	if *agg.Value != float64(985) {
		t.Fatalf("expected aggregation value = %v; got: %v", float64(985), *agg.Value)
	}
}

func TestAggsPipelineMaxBucket(t *testing.T) {
	s := `{
	"max_monthly_sales" : {
		"keys": ["2015/01/01 00:00:00"],
	  "value" : 550
  }
}`

	aggs := new(Aggregations)
	err := json.Unmarshal([]byte(s), &aggs)
	if err != nil {
		t.Fatalf("expected no error decoding; got: %v", err)
	}

	agg, found := aggs.MaxBucket("max_monthly_sales")
	if !found {
		t.Fatalf("expected aggregation to be found; got: %v", found)
	}
	if agg == nil {
		t.Fatalf("expected aggregation != nil; got: %v", agg)
	}
	if len(agg.Keys) != 1 {
		t.Fatalf("expected 1 key; got: %d", len(agg.Keys))
	}
	if got, want := agg.Keys[0], "2015/01/01 00:00:00"; got != want {
		t.Fatalf("expected key %q; got: %v (%T)", want, got, got)
	}
	if agg.Value == nil {
		t.Fatalf("expected aggregation value != nil; got: %v", agg.Value)
	}
	if *agg.Value != float64(550) {
		t.Fatalf("expected aggregation value = %v; got: %v", float64(550), *agg.Value)
	}
}

func TestAggsPipelineMinBucket(t *testing.T) {
	s := `{
	"min_monthly_sales" : {
		"keys": ["2015/02/01 00:00:00"],
	  "value" : 60
  }
}`

	aggs := new(Aggregations)
	err := json.Unmarshal([]byte(s), &aggs)
	if err != nil {
		t.Fatalf("expected no error decoding; got: %v", err)
	}

	agg, found := aggs.MinBucket("min_monthly_sales")
	if !found {
		t.Fatalf("expected aggregation to be found; got: %v", found)
	}
	if agg == nil {
		t.Fatalf("expected aggregation != nil; got: %v", agg)
	}
	if len(agg.Keys) != 1 {
		t.Fatalf("expected 1 key; got: %d", len(agg.Keys))
	}
	if got, want := agg.Keys[0], "2015/02/01 00:00:00"; got != want {
		t.Fatalf("expected key %q; got: %v (%T)", want, got, got)
	}
	if agg.Value == nil {
		t.Fatalf("expected aggregation value != nil; got: %v", agg.Value)
	}
	if *agg.Value != float64(60) {
		t.Fatalf("expected aggregation value = %v; got: %v", float64(60), *agg.Value)
	}
}

func TestAggsPipelineMovAvg(t *testing.T) {
	s := `{
	"the_movavg" : {
	  "value" : 12.0
  }
}`

	aggs := new(Aggregations)
	err := json.Unmarshal([]byte(s), &aggs)
	if err != nil {
		t.Fatalf("expected no error decoding; got: %v", err)
	}

	agg, found := aggs.MovAvg("the_movavg")
	if !found {
		t.Fatalf("expected aggregation to be found; got: %v", found)
	}
	if agg == nil {
		t.Fatalf("expected aggregation != nil; got: %v", agg)
	}
	if agg.Value == nil {
		t.Fatalf("expected aggregation value != nil; got: %v", agg.Value)
	}
	if *agg.Value != float64(12.0) {
		t.Fatalf("expected aggregation value = %v; got: %v", float64(12.0), *agg.Value)
	}
}

func TestAggsPipelineDerivative(t *testing.T) {
	s := `{
	"sales_deriv" : {
	  "value" : 315
  }
}`

	aggs := new(Aggregations)
	err := json.Unmarshal([]byte(s), &aggs)
	if err != nil {
		t.Fatalf("expected no error decoding; got: %v", err)
	}

	agg, found := aggs.Derivative("sales_deriv")
	if !found {
		t.Fatalf("expected aggregation to be found; got: %v", found)
	}
	if agg == nil {
		t.Fatalf("expected aggregation != nil; got: %v", agg)
	}
	if agg.Value == nil {
		t.Fatalf("expected aggregation value != nil; got: %v", agg.Value)
	}
	if *agg.Value != float64(315) {
		t.Fatalf("expected aggregation value = %v; got: %v", float64(315), *agg.Value)
	}
}

func TestAggsPipelineStatsBucket(t *testing.T) {
	s := `{
	"stats_monthly_sales": {
	 "count": 3,
	 "min": 60.0,
	 "max": 550.0,
	 "avg": 328.3333333333333,
	 "sum": 985.0
  }
}`

	aggs := new(Aggregations)
	err := json.Unmarshal([]byte(s), &aggs)
	if err != nil {
		t.Fatalf("expected no error decoding; got: %v", err)
	}

	agg, found := aggs.StatsBucket("stats_monthly_sales")
	if !found {
		t.Fatalf("expected aggregation to be found; got: %v", found)
	}
	if agg == nil {
		t.Fatalf("expected aggregation != nil; got: %v", agg)
	}
	if agg.Count != 3 {
		t.Fatalf("expected aggregation count = %v; got: %v", 3, agg.Count)
	}
	if agg.Min == nil {
		t.Fatalf("expected aggregation min != nil; got: %v", agg.Min)
	}
	if *agg.Min != float64(60.0) {
		t.Fatalf("expected aggregation min = %v; got: %v", float64(60.0), *agg.Min)
	}
	if agg.Max == nil {
		t.Fatalf("expected aggregation max != nil; got: %v", agg.Max)
	}
	if *agg.Max != float64(550.0) {
		t.Fatalf("expected aggregation max = %v; got: %v", float64(550.0), *agg.Max)
	}
	if agg.Avg == nil {
		t.Fatalf("expected aggregation avg != nil; got: %v", agg.Avg)
	}
	if *agg.Avg != float64(328.3333333333333) {
		t.Fatalf("expected aggregation average = %v; got: %v", float64(328.3333333333333), *agg.Avg)
	}
	if agg.Sum == nil {
		t.Fatalf("expected aggregation sum != nil; got: %v", agg.Sum)
	}
	if *agg.Sum != float64(985.0) {
		t.Fatalf("expected aggregation sum = %v; got: %v", float64(985.0), *agg.Sum)
	}
}

func TestAggsPipelineCumulativeSum(t *testing.T) {
	s := `{
	"cumulative_sales" : {
	  "value" : 550
  }
}`

	aggs := new(Aggregations)
	err := json.Unmarshal([]byte(s), &aggs)
	if err != nil {
		t.Fatalf("expected no error decoding; got: %v", err)
	}

	agg, found := aggs.CumulativeSum("cumulative_sales")
	if !found {
		t.Fatalf("expected aggregation to be found; got: %v", found)
	}
	if agg == nil {
		t.Fatalf("expected aggregation != nil; got: %v", agg)
	}
	if agg.Value == nil {
		t.Fatalf("expected aggregation value != nil; got: %v", agg.Value)
	}
	if *agg.Value != float64(550) {
		t.Fatalf("expected aggregation value = %v; got: %v", float64(550), *agg.Value)
	}
}

func TestAggsPipelineBucketScript(t *testing.T) {
	s := `{
	"t-shirt-percentage" : {
	  "value" : 20
  }
}`

	aggs := new(Aggregations)
	err := json.Unmarshal([]byte(s), &aggs)
	if err != nil {
		t.Fatalf("expected no error decoding; got: %v", err)
	}

	agg, found := aggs.BucketScript("t-shirt-percentage")
	if !found {
		t.Fatalf("expected aggregation to be found; got: %v", found)
	}
	if agg == nil {
		t.Fatalf("expected aggregation != nil; got: %v", agg)
	}
	if agg.Value == nil {
		t.Fatalf("expected aggregation value != nil; got: %v", agg.Value)
	}
	if *agg.Value != float64(20) {
		t.Fatalf("expected aggregation value = %v; got: %v", float64(20), *agg.Value)
	}
}

func TestAggsPipelineSerialDiff(t *testing.T) {
	s := `{
	"the_diff" : {
	  "value" : -722.0
  }
}`

	aggs := new(Aggregations)
	err := json.Unmarshal([]byte(s), &aggs)
	if err != nil {
		t.Fatalf("expected no error decoding; got: %v", err)
	}

	agg, found := aggs.SerialDiff("the_diff")
	if !found {
		t.Fatalf("expected aggregation to be found; got: %v", found)
	}
	if agg == nil {
		t.Fatalf("expected aggregation != nil; got: %v", agg)
	}
	if agg.Value == nil {
		t.Fatalf("expected aggregation value != nil; got: %v", agg.Value)
	}
	if *agg.Value != float64(-722.0) {
		t.Fatalf("expected aggregation value = %v; got: %v", float64(20), *agg.Value)
	}
}
