bash makeDir.sh $1 $2
((totalSize = $2 * $3))
echo "generating data, total size: $totalSize G"
cd ..
./dbgen -f -s $totalSize
cd ./scripts

i=0
while ((i < $2))
do
	echo "separating data, running for database$i"
	((a = i * 150000 + 1))
	((b = (i + 1) * 150000))
	sed -n "$a,$b"p ../customer.tbl > $1/database$i/customer.tbl

	((a = i * 6000000 + 1))
	((b = (i + 1) * 6000000))
	sed -n "$a,$b"p ../lineitem.tbl > $1/database$i/lineitem.tbl

	((a = i * 6000000 + 1))
	((b = (i + 1) * 6000000))
	sed -n "$a,$b"p ../orders.tbl > $1/database$i/orders.tbl

	((a = i * 200000 + 1))
	((b = (i + 1) * 200000))
	sed -n "$a,$b"p ../part.tbl > $1/database$i/part.tbl

	((a = i * 800000 + 1))
	((b = (i + 1) * 800000))
	sed -n "$a,$b"p ../partsupp.tbl > $1/database$i/partsupp.tbl

	((a = i * 10000 + 1))
	((b = (i + 1) * 10000))
	sed -n "$a,$b"p ../supplier.tbl > $1/database$i/supplier.tbl

	cp ../nation.tbl $1/database$i
	cp ../region.tbl $1/database$i
	((i++))
done
rm ../*.tbl
