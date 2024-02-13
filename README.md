# fedup-experiments

FedUP is a SPARQL federation engine that builds logical plans based on
*Result-Aware* source selection. These plans looks like
unions-over-joins trees while state-of-the-art engines builds
joins-over-unions trees [1]. This project aims to evaluate the impact of
such a shift on two benchmarks: 

- [X] The traditional LargeRDFBench benchmark [2] that comprises a dozen
      of endpoints.
- [X] The new FedShop benchmark [3] that emulates an e-commerce
      application that scales up to 200 endpoints.

_A better version of FedUP has been developped since then, and it's
available at its [dedicated
repository](https://github.com/GDD-Nantes/fedup)._

## Installation

> **Note**
> Installation instructions have only been tested on Ubuntu 20.04.6 LTS

> **Dependencies**
> conda, maven, java 11 & 20 (JDK)

```bash
git clone git@github.com:GDD-Nantes/FedUP-experiments.git
cd FedUP-experiments
    
conda env create -f environment.yaml
conda activate fedupxp
    
snakemake -c1 -R run_xp
```

## Experimental Results

Experimental results and plot scripts are available in the
[results](https://github.com/GDD-Nantes/FedUP-experiments/tree/main/results)
directory. Detailed interpretations are available in the article.

![Total execution time on LargeRDFBench.](results/total_largerdfbench.png)

![Total execution time on FedShop.](results/total_fedshop_20_200.png)


## Supplementary Materials

Due to space limitations, these experimental results do not appear in
the scientific paper.  All measurements for
[LargeRDFBench](results/results_largerdfbench.csv) and [FedShop on
batches 0 and 9](results/results_fedshop.csv) are available in the
form of `.csv` files.

### Source Selection Times

Source selection times of LargeRDFBench and FedShop queries on a
logarithmic scale. Execution times and source selection times cannot
coexist on the same stacked bar chart as the logarithmic scale
misleads the reader to think that either source selection times do not
exist (when they are on top), or they are very important (when they
are at the bottom).

> **Careful**
> The scale of execution times goes down to 0.001 while this scale for source selection times goes down to 0.0001.

![image](results/largerdfbench_source_selection_time.png)

![image](results/fedshop_source_selection_time_batch_0_9.png)

### Total Execution Times on all FedShop's Configurations

![image](results/exec_time.png)

### Source Selection Times on all FedShop's Configurations

![image](results/source_selection_time.png)

### Data Transfer on all FedShop's Configurations

![image](results/data_transfer.png)

### FedUP vs State-of-the-art on all FedShop's Configurations 

> **Careful** Any metric column should be interpreted within the
> context of "status_timeout" and "status_error".  For example, for
> CostFed the data transfer appears to "diminish" as federation size
> grows because the timeout rate increases.

![image](results/fedup_fedshop_extra.png)


## References

[1] Sijin Cheng and Olaf Hartig. __FedQPL: A language for logical
query plans over heterogeneous federations of RDF data sources.__ In
the 22nd International Conference on Information Integration and
Web-Based Applications & Services. 2021.

[2] Michael Schmidt,Olaf Görlitz,Peter Haase,Günter Ladwig, Andreas
Schwarte, and Thanh Tran. __FedBench: A benchmark suite for federated
semantic data query processing.__ In 10th International Semantic Web
Conference (ISWC). 2011

[3] Minh-Hoang Dang, Julien Aimonier-Davat, Pascal Molli, Olaf Hartig,
Hala Skaf-Molli, and Yotlan Le Crom. __FedShop: A benchmark for
testing the scalability of SPARQL federation engines.__ In
International Semantic Web Conference (ISWC). 2023.
