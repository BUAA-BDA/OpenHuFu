bash ./scripts/test/makeDir.sh $1 $2
((totalSize = $2 * $3))
echo "generating data, total size: $totalSize G"
cd ./dataset/TPC-H\ V3.0.1/dbgen/
./dbgen -f -s $totalSize

pwd
i=0
while ((i < $2))
do
	echo "separating data, running for database$i"
	((a = i * 150000 + 1))
	((b = (i + 1) * 150000))
	sed -n "$a,$b"p ./customer.tbl | sed 's/.$//' > $1/database$i/customer.tbl

	((a = i * 6000000 + 1))
	((b = (i + 1) * 6000000))
	sed -n "$a,$b"p ./lineitem.tbl | sed 's/.$//' > $1/database$i/lineitem.tbl

	((a = i * 6000000 + 1))
	((b = (i + 1) * 6000000))
	sed -n "$a,$b"p ./orders.tbl | sed 's/.$//' > $1/database$i/orders.tbl

	((a = i * 200000 + 1))
	((b = (i + 1) * 200000))
	sed -n "$a,$b"p ./part.tbl | sed 's/.$//' > $1/database$i/part.tbl

	((a = i * 800000 + 1))
	((b = (i + 1) * 800000))
	sed -n "$a,$b"p ./partsupp.tbl | sed 's/.$//' > $1/database$i/partsupp.tbl

	((a = i * 10000 + 1))
	((b = (i + 1) * 10000))
	sed -n "$a,$b"p ./supplier.tbl | sed 's/.$//' > $1/database$i/supplier.tbl

	sed 's/.$//' ./nation.tbl > $1/database$i/nation.tbl
	sed 's/.$//' ./region.tbl > $1/database$i/region.tbl
	((i++))
done
rm ./*.tbl
