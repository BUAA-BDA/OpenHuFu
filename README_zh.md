# OpenHuFu：开源数据联邦系统

[![codecov](https://codecov.io/gh/BUAA-BDA/OpenHuFu/branch/main/graph/badge.svg?token=QJBEGGNL2P)](https://codecov.io/gh/BUAA-BDA/OpenHuFu)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![Total Lines](https://tokei.rs/b1/github/BUAA-BDA/OpenHuFu?category=lines)](https://github.com/BUAA-BDA/OpenHuFu)

由于安全方面的顾虑，不同数据所有者通常无法直接共享原始数据，数据孤岛因而成为大数据查询处理规模化发展的障碍。
一种具有前景的解决方案，是利用安全多方计算（Secure Multi-Party Computation，SMC）、差分隐私等技术，在多个数据所有者组成的数据联邦上执行安全查询与分析。近年来，数据联邦和联邦学习领域的相关研究已验证了这一思路的可行性。

OpenHuFu是一个面向数据联邦高效、安全查询处理的开源系统。
它为研究人员提供了灵活的开发环境，便于快速实现基于秘密共享、混淆电路、不经意传输等SMC技术的联邦查询处理算法。
借助OpenHuFu，研究人员可以在基准数据集上快速开展实验评估，并测试所设计算法的性能。

## 从源代码编译OpenHuFu

### 环境要求

- Linux或macOS
- Java 11
- Maven 3.5.2及以上版本
- C++（用于生成TPC-H数据）
- Python 3（用于生成空间数据）
- Git与Git LFS（Git大文件存储）

### 构建OpenHuFu

执行以下命令：

1. 克隆OpenHuFu代码仓库：

```shell
git clone https://github.com/BUAA-BDA/OpenHuFu.git
```

2. 从Git LFS下载大文件：

```shell
cd OpenHuFu
git lfs install --skip-smudge
git lfs pull
```

3. 构建项目：

```shell
cd OpenHuFu
bash scripts/build/package.sh
```

构建完成后，OpenHuFu将安装在`release`目录中。

### 注意事项

在macOS上使用时，需要将以下配置添加到Maven配置文件`settings.xml`中：

```xml
<profiles>
    <profile>
      <id>macos</id>
      <properties>
        <os.detected.classifier>osx-x86_64</os.detected.classifier>
      </properties>
    </profile>
</profiles>
<activeProfiles>
    <activeProfile>macos</activeProfile>
</activeProfiles>
```

## 数据生成

### 关系数据：[TPC-H](https://www.tpc.org/tpch/)

#### 使用方法

```shell
bash scripts/test/extract_tpc_h.sh

cd dataset/TPC-H\ V3.0.1/dbgen
cp makefile.suite makefile
# 在macOS上，需要将dbgen中的“#include <malloc.h>”替换为“#include <sys/malloc.h>”
make

# 返回项目根目录
cd ../../..
# x表示数据库数量，y表示每个数据库的数据量（MB）
bash scripts/test/generateData.sh x y
```

### 空间数据

空间示例数据位于`dataset/newyork-taxi-sample.data`。

#### 使用方法

生成空间数据：

```shell
pip3 install numpy
python3 scripts/test/genSyntheticData.py databaseNum dataSize [distribution name] [params]
```

目前支持的分布及其参数如下：

| 分布 | 参数1 | 参数2 |
| :---: | :---: | :---: |
| `uni` | `low`（默认值为`-1e7`） | `high`（默认值为`1e7`） |
| `nor` | `mu`（默认值为`0`） | `sigma`（默认值为`1e5`） |
| `exp` | `mu`（默认值为`5e6`） |  |

如有需要，可以修改`scripts/test/genSyntheticData.py`。

### 注意事项

每张表由CSV和SCM两种格式的文件共同定义，文件名即为实际表名。<br/>
CSV文件包含列名和表数据，SCM文件包含列名和列类型。不同列字段之间使用分隔符进行划分，该分隔符可以在数据所有者的配置文件中指定。

## 配置文件

### 数据所有者端

### 用户端

## 开发流程

1. 开发算法。

- 聚合算法：

```java
class extends com.hufudb.openhufu.owner.implementor.aggregate.OwnerAggregateFunction
/**
 * 该类必须包含一个具有以下参数的构造函数：
 * (OpenHuFuPlan.Expression agg, Rpc rpc, ExecutorService threadPool, OpenHuFuPlan.TaskInfo taskInfo)
 */
```

- 连接算法：

```java
class implements com.hufudb.openhufu.owner.implementor.join.OwnerJoin
```

2. 为查询设置相应算法，以下为`owner.yaml`中的配置示例：

```yaml
openhufu:
    implementor:
      aggregate:
        sum: com.hufudb.openhufu.owner.implementor.aggregate.sum.SecretSharingSum
        count: null
        max: null
        min: null
        avg: null
      join: com.hufudb.openhufu.owner.implementor.join.HashJoin
```

3. 构建OpenHuFu。

   按照“构建OpenHuFu”一节中的说明构建项目。

4. 运行OpenHuFu。

   `release/config`目录提供了三个数据所有者的示例配置。<br/>
   可以使用这些配置在单台机器上运行演示，也可以修改配置文件，将OpenHuFu部署到多台机器上。<br/>

   由于配置文件使用相对路径，运行命令前需要先执行`cd release`。

   在单台机器上运行演示：

   ```shell
   bash owner_all.sh
   ```

   在多台机器上运行OpenHuFu：

   ```shell
   bash owner.sh start ./config/owner{i}.json
   ```

   停止OpenHuFu：

   ```shell
   bash owner.sh stop
   ```

5. 运行基准测试。

```shell
bash benchmark.sh
```

6. 评估通信开销。

在OpenHuFu上运行基准测试前，可以按照以下说明评估查询的通信开销。

- 监控端口：

```shell
# 以root权限运行该Shell脚本
# 8888为端口号
sudo bash scripts/test/network_mmonitor/start.sh 8888
```

- 计算通信开销：

```shell
# 以root权限运行该Shell脚本
sudo bash scripts/test/network_mmonitor/monitor.sh
```

## 数据查询语言

1. 查询计划
2. 函数调用

## 支持的查询类型

- 过滤
- 投影
- 连接
  - 等值连接
  - θ连接
- 笛卡尔积
- 聚合（包括分组聚合）
- 有限窗口聚合
- 去重
- 排序
- 限制返回结果数量
- 公用表表达式
- 空间查询
  - 范围查询
  - 范围计数
  - K近邻查询
  - 距离连接
  - K近邻连接

## 评估指标

- 通信开销
- 运行时间
  - 查询总时间
  - 本地查询时间
  - 加密时间
  - 解密时间

## 相关工作

**如果OpenHuFu对你的研究有所帮助，欢迎引用我们的论文。相应的BibTeX信息如下：**

1. **Hu-Fu: Efficient and Secure Spatial Queries over Data Federation.**
   *Yongxin Tong, Xuchen Pan, Yuxiang Zeng, Yexuan Shi, Chunbo Xue, Zimu Zhou, Xiaofei Zhang, Lei Chen, Yi Xu, Ke Xu, Weifeng Lv.* Proc. VLDB Endow. 15(6): 1159-1172 (2022). \[[论文](https://www.vldb.org/pvldb/vol15/p1159-tong.pdf)\] \[[幻灯片](http://yongxintong.group/static/paper/2018/VLDB2018_A%20Unified%20Approach%20to%20Route%20Planning%20for%20Shared%20Mobility_Slides.pptx)\] \[[BibTeX](https://dblp.org/rec/journals/pvldb/TongPZSXZZCXXL22.html?view=bibtex)\]

**本研究团队的其他相关工作如下：**

1. **Efficient Approximate Range Aggregation Over Large-Scale Spatial Data Federation.**
   *Yexuan Shi, Yongxin Tong, Yuxiang Zeng, Zimu Zhou, Bolin Ding, Lei Chen.* IEEE Trans. Knowl. Data Eng. 35(1): 418-430 (2023). \[[论文](https://hufudb.com/static/paper/2022/TKDE2022_Efficient%20Approximate%20Range%20Aggregation%20over%20Large-scale%20Spatial%20Data%20Federation.pdf)\] \[[BibTeX](https://dblp.org/rec/journals/tkde/ShiTZZDC23.html?view=bibtex)\]

2. **Hu-Fu: A Data Federation System for Secure Spatial Queries.**
   *Xuchen Pan, Yongxin Tong, Chunbo Xue, Zimu Zhou, Junping Du, Yuxiang Zeng, Yexuan Shi, Xiaofei Zhang, Lei Chen, Yi Xu, Ke Xu, Weifeng Lv.* Proc. VLDB Endow. 15(12): 3582-3585 (2022). \[[论文](https://www.vldb.org/pvldb/vol15/p3582-tong.pdf)\] \[[BibTeX](https://dblp.org/rec/journals/pvldb/PanTXZDZSZCXXL22.html?view=bibtex)\]

3. **Data Source Selection in Federated Learning: A Submodular Optimization Approach.**
   *Ruisheng Zhang, Yansheng Wang, Zimu Zhou, Ziyao Ren, Yongxin Tong, Ke Xu.* DASFAA 2022. \[[论文](https://doi.org/10.1007/978-3-031-00126-0_43)\] \[[BibTeX](https://dblp.org/rec/conf/dasfaa/ZhangWZRTX22.html?view=bibtex)\]

4. **Fed-LTD: Towards Cross-Platform Ride Hailing via Federated Learning to Dispatch.**
   *Yansheng Wang, Yongxin Tong, Zimu Zhou, Ziyao Ren, Yi Xu, Guobin Wu, Weifeng Lv.* KDD 2022. \[[论文](https://doi.org/10.1145/3534678.3539047)\] \[[BibTeX](https://dblp.org/rec/conf/kdd/WangTZRXWL22.html?view=bibtex)\]

5. **Efficient and Secure Skyline Queries over Vertical Data Federation.**
   *Yuanyuan Zhang, Yexuan Shi, Zimu Zhou, Chunbo Xue, Yi Xu, Ke Xu, Junping Du.* IEEE Trans. Knowl. Data Eng. (2022). \[[论文](https://doi.org/10.1109/TKDE.2022.3222415)\] \[[BibTeX](https://ieeexplore.ieee.org/document/9950625)\]

6. **Federated Topic Discovery: A Semantic Consistent Approach.**
   *Yexuan Shi, Yongxin Tong, Zhiyang Su, Di Jiang, Zimu Zhou, Wenbin Zhang.* IEEE Intell. Syst. 36(5): 96-103 (2021). \[[论文](https://doi.org/10.1109/MIS.2020.3033459)\] \[[BibTeX](https://dblp.org/rec/journals/expert/ShiTSJZZ21.html?view=bibtex)\]

7. **Industrial Federated Topic Modeling.**
   *Di Jiang, Yongxin Tong, Yuanfeng Song, Xueyang Wu, Weiwei Zhao, Jinhua Peng, Rongzhong Lian, Qian Xu, Qiang Yang.* ACM Trans. Intell. Syst. Technol. 12(1): 2:1-2:22 (2021). \[[论文](https://doi.org/10.1145/3418283)\] \[[BibTeX](https://dblp.org/rec/journals/tist/JiangTSWZPLXY21.html?view=bibtex)\]

8. **A GDPR-compliant Ecosystem for Speech Recognition with Transfer, Federated, and Evolutionary Learning.**
   *Di Jiang, Conghui Tan, Jinhua Peng, Chaotao Chen, Xueyang Wu, Weiwei Zhao, Yuanfeng Song, Yongxin Tong, Chang Liu, Qian Xu, Qiang Yang, Li Deng.* ACM Trans. Intell. Syst. Technol. 12(3): 30:1-30:19 (2021). \[[论文](https://doi.org/10.1145/3447687)\] \[[BibTeX](https://dblp.org/rec/journals/tist/JiangTPCWZSTLXY21.html?view=bibtex)\]

9. **An Efficient Approach for Cross-Silo Federated Learning to Rank.**
   *Yansheng Wang, Yongxin Tong, Dingyuan Shi, Ke Xu.* ICDE 2021. \[[论文](https://doi.org/10.1109/ICDE51399.2021.00102)\] \[[幻灯片](https://hufudb.com/static/paper/2021/ICDE2021_An%20Efficient%20Approach%20for%20Cross-Silo%20Federated%20Learning%20to%20Rank_Slides.pptx)\] \[[BibTeX](https://dblp.org/rec/conf/icde/WangTS021.html?view=bibtex)\]

10. **Federated Learning in the Lens of Crowdsourcing.**
    *Yongxin Tong, Yansheng Wang, Dingyuan Shi.* IEEE Data Eng. Bull. 43(3): 26-36 (2020). \[[论文](http://sites.computer.org/debull/A20sept/p26.pdf)\] \[[BibTeX](https://dblp.org/rec/journals/debu/TongWS20.html?view=bibtex)\]

11. **Federated Latent Dirichlet Allocation: A Local Differential Privacy Based Framework.**
    *Yansheng Wang, Yongxin Tong, Dingyuan Shi.* AAAI 2020. \[[论文](https://ojs.aaai.org/index.php/AAAI/article/view/6096)\] \[[BibTeX](https://dblp.org/rec/conf/aaai/WangTS20.html?view=bibtex)\]

12. **Federated Acoustic Model Optimization for Automatic Speech Recognition.**
    *Conghui Tan, Di Jiang, Huaxiao Mo, Jinhua Peng, Yongxin Tong, Weiwei Zhao, Chaotao Chen, Rongzhong Lian, Yuanfeng Song, Qian Xu.* DASFAA 2020. \[[论文](https://doi.org/10.1007/978-3-030-59419-0_54)\] \[[BibTeX](https://dblp.org/rec/conf/dasfaa/TanJMPTZCLSX20.html?view=bibtex)\]

13. **Efficient and Fair Data Valuation for Horizontal Federated Learning.**
    *Shuyue Wei, Yongxin Tong, Zimu Zhou, Tianshu Song.* Federated Learning 2020. \[[论文](https://doi.org/10.1007/978-3-030-63076-8_10)\] \[[BibTeX](https://dblp.org/rec/series/lncs/WeiTZS20.html?view=bibtex)\]

14. **Profit Allocation for Federated Learning.**
    *Tianshu Song, Yongxin Tong, Shuyue Wei.* IEEE BigData 2019. \[[论文](https://doi.org/10.1109/BigData47090.2019.9006327)\] \[[幻灯片](https://hufudb.com/static/paper/2019/BigData2019_Profit%20Allocation%20for%20Federated%20Learning_Slides.pptx)\] \[[BibTeX](https://dblp.org/rec/conf/bigdataconf/SongTW19.html?view=bibtex)\]

15. **Federated Machine Learning: Concept and Applications.**
    *Qiang Yang, Yang Liu, Tianjian Chen, Yongxin Tong.* ACM Trans. Intell. Syst. Technol. 10(2): 12:1-12:19 (2019). \[[论文](https://doi.org/10.1145/3298981)\] \[[BibTeX](https://dblp.org/rec/journals/tist/YangLCT19.html?view=bibtex)\]
