# OpenHuFu: An Open-Sourced Data Federation System

Data isolation has become an obstacle to scale up query processing over big data, since sharing raw data among data owners is often prohibitive due to security concerns. A promising solution is to perform secure queries and analytics over a federation of multiple data owners leveraging techiniques like secure multi-party computation (SMC) and differential privacy, as evidenced by recent work on data federation and federated learning. 

OpenHuFu is an open-sourced system for efficient and secure query processing on a data federation.
It provides flexibility for researchers to quickly implement their algorithms for processing federated queries with SMC techniques, such as secret sharing, garbled circuit and oblivious transfer.
With its help, we can quickly conduct the experimental evaluation and obtain the performance of the designed algorithms over benchmark datasets.

## Building OpenHuFu from Source

prerequisites:

- Linux
- Java 11
- Maven (version at least 3.5.2)

Run the following commands

```cmd
git clone https://github.com/BUAA-BDA/OpenHuFu.git
cd OpenHuFu
./build/script/package.sh
```

OpenHuFu is now installed in `release`


## Data Generation

[TCP-H](https://www.tpc.org/tpch/)
### How to use it
```cmd
cd dataset/TPC-H V3.0.1/dbgen
cp makefile.suite makefile
make
cd scripts
# dst is the target folder, x is the number of database，y is the volume of each database
bash generateData.sh dst x y
```

## Configuration File
### OwnerSide

### UserSide


## Running OpenHuFu




## Data Query Language

1. Plan
2. Function Call: Hard to describe query

## Query Type

* Filter
* Projection
* Join: equi-join, theta join 
* Cross products
* Aggregate(inc. group-by)
* Limited window aggs
* Distinct
* Sort
* Limit
* Common table expressions
* Spatial Queries:
  * range query
  * range counting
  * knn query
  * distance join
  * knn join

## Evaluation Metrics

* Communication Cost
* Running Time
  * Data Access Time
  * Encryption Time
  * Decryption Time
  * Query Time

## Related Work

**If you find OpenHuFu helpful in your research, please consider citing our papers and the bibtex are listed below:**

1. **Hu-Fu: Efficient and Secure Spatial Queries over Data Federation.**
*Yongxin Tong, Xuchen Pan, Yuxiang Zeng, Yexuan Shi, Chunbo Xue, Zimu Zhou, Xiaofei Zhang, Lei Chen, Yi Xu, Ke Xu, Weifeng Lv.* Proc. VLDB Endow. 15(6): 1159-1172 (2022). \[[paper](https://www.vldb.org/pvldb/vol15/p1159-tong.pdf)\] \[[slides](http://yongxintong.group/static/paper/2018/VLDB2018_A%20Unified%20Approach%20to%20Route%20Planning%20for%20Shared%20Mobility_Slides.pptx)\] \[[bibtex](https://dblp.org/rec/journals/pvldb/TongPZSXZZCXXL22.html?view=bibtex)\]

**Other helpful related work from our group is listed below:**

1. **Efficient Approximate Range Aggregation Over Large-Scale Spatial Data Federation.**
*Yexuan Shi, Yongxin Tong, Yuxiang Zeng, Zimu Zhou, Bolin Ding, Lei Chen.* Proc. VLDB Endow. 15(6): 1159-1172 (2022). \[[paper](https://hufudb.com/static/paper/2022/TKDE2022_Efficient%20Approximate%20Range%20Aggregation%20over%20Large-scale%20Spatial%20Data%20Federation.pdf)\] \[[bibtex](https://dblp.org/rec/journals/tkde/ShiTZZDC23.html?view=bibtex)\]

1. **Hu-Fu: A Data Federation System for Secure Spatial Queries.**
*Xuchen Pan, Yongxin Tong, Chunbo Xue, Zimu Zhou, Junping Du, Yuxiang Zeng, Yexuan Shi, Xiaofei Zhang, Lei Chen, Yi Xu, Ke Xu, Weifeng Lv.* Proc. VLDB Endow. 15(12): 3582-3585 (2022). \[[paper](https://www.vldb.org/pvldb/vol15/p3582-tong.pdf)\] \[[bibtex](https://dblp.org/rec/journals/pvldb/PanTXZDZSZCXXL22.html?view=bibtex)\]

1. **Data Source Selection in Federated Learning: A Submodular Optimization Approach.**
*Ruisheng Zhang, Yansheng Wang, Zimu Zhou, Ziyao Ren, Yongxin Tong, Ke Xu.* DASFAA 2022. \[[paper](https://doi.org/10.1007/978-3-031-00126-0_43)\] \[[bibtex](https://dblp.org/rec/conf/dasfaa/ZhangWZRTX22.html?view=bibtex)\]

1. **Fed-LTD: Towards Cross-Platform Ride Hailing via Federated Learning to Dispatch.**
*Yansheng Wang, Yongxin Tong, Zimu Zhou, Ziyao Ren, Yi Xu, Guobin Wu, Weifeng Lv.* KDD 2022. \[[paper](https://doi.org/10.1145/3534678.3539047)\] \[[bibtex](https://dblp.org/rec/conf/kdd/WangTZRXWL22.html?view=bibtex)\]

1. **Federated Topic Discovery: A Semantic Consistent Approach.**
*Yexuan Shi, Yongxin Tong, Zhiyang Su, Di Jiang, Zimu Zhou, Wenbin Zhang.* IEEE Intell. Syst. 36(5): 96-103 (2021). \[[paper](https://doi.org/10.1109/MIS.2020.3033459)\] \[[bibtex](https://dblp.org/rec/journals/expert/ShiTSJZZ21.html?view=bibtex)\]

1. **Industrial Federated Topic Modeling.**
*Di Jiang, Yongxin Tong, Yuanfeng Song, Xueyang Wu, Weiwei Zhao, Jinhua Peng, Rongzhong Lian, Qian Xu, Qiang Yang.* ACM Trans. Intell. Syst. Technol. 12(1): 2:1-2:22 (2021). \[[paper](https://doi.org/10.1145/3418283)\] \[[bibtex](https://dblp.org/rec/journals/tist/JiangTSWZPLXY21.html?view=bibtex)\]

1. **A GDPR-compliant Ecosystem for Speech Recognition with Transfer, Federated, and Evolutionary Learning.**
*Di Jiang, Conghui Tan, Jinhua Peng, Chaotao Chen, Xueyang Wu, Weiwei Zhao, Yuanfeng Song, Yongxin Tong, Chang Liu, Qian Xu, Qiang Yang, Li Deng.* ACM Trans. Intell. Syst. Technol. 12(3): 30:1-30:19 (2021). \[[paper](https://doi.org/10.1145/3447687)\] \[[bibtex](https://dblp.org/rec/journals/tist/JiangTPCWZSTLXY21.html?view=bibtex)\]

1. **An Efficient Approach for Cross-Silo Federated Learning to Rank.**
*Yansheng Wang, Yongxin Tong, Dingyuan Shi, Ke Xu.* ICDE 2021. \[[paper](https://doi.org/10.1109/ICDE51399.2021.00102)\] \[[slides](https://hufudb.com/static/paper/2021/ICDE2021_An%20Efficient%20Approach%20for%20Cross-Silo%20Federated%20Learning%20to%20Rank.pptx)\] \[[bibtex](https://dblp.org/rec/conf/icde/WangTS021.html?view=bibtex)\]

1. **Federated Learning in the Lens of Crowdsourcing.**
*Yongxin Tong, Yansheng Wang, Dingyuan Shi.* IEEE Data Eng. Bull. 43(3): 26-36 (2020). \[[paper](http://sites.computer.org/debull/A20sept/p26.pdf)\] \[[bibtex](https://dblp.org/rec/journals/debu/TongWS20.html?view=bibtex)\]

1. **Federated Latent Dirichlet Allocation: A Local Differential Privacy Based Framework.**
*Yansheng Wang, Yongxin Tong, Dingyuan Shi.* AAAI 2020. \[[paper](https://ojs.aaai.org/index.php/AAAI/article/view/6096)\] \[[bibtex](https://dblp.org/rec/conf/aaai/WangTS20.html?view=bibtex)\]

1. **Federated Acoustic Model Optimization for Automatic Speech Recognition.**
*Conghui Tan, Di Jiang, Huaxiao Mo, Jinhua Peng, Yongxin Tong, Weiwei Zhao, Chaotao Chen, Rongzhong Lian, Yuanfeng Song, Qian Xu.* DASFAA 2020. \[[paper](https://doi.org/10.1007/978-3-030-59419-0_54)\] \[[bibtex](https://dblp.org/rec/conf/dasfaa/TanJMPTZCLSX20.html?view=bibtex)\]

1. **Efficient and Fair Data Valuation for Horizontal Federated Learning.**
*Shuyue Wei, Yongxin Tong, Zimu Zhou, Tianshu Song.* Federated Learning 2020. \[[paper](https://doi.org/10.1007/978-3-030-63076-8_10)\] \[[bibtex](https://dblp.org/rec/series/lncs/WeiTZS20.html?view=bibtex)\]

1. **Profit Allocation for Federated Learning.**
*Tianshu Song, Yongxin Tong, Shuyue Wei.* IEEE BigData 2019. \[[paper](https://doi.org/10.1109/BigData47090.2019.9006327)\] \[[slides](https://hufudb.com/static/paper/2019/BigData2019_Profit%20Allocation%20for%20Federated%20Learning_Slides.pptx)\] \[[bibtex](https://dblp.org/rec/conf/bigdataconf/SongTW19.html?view=bibtex)\]

1. **Federated Machine Learning: Concept and Applications.**
*Qiang Yang, Yang Liu, Tianjian Chen, Yongxin Tong.* ACM Trans. Intell. Syst. Technol. 10(2): 12:1-12:19 (2019). \[[paper](https://doi.org/10.1145/3298981)\] \[[bibtex](https://dblp.org/rec/journals/tist/YangLCT19.html?view=bibtex)\]


