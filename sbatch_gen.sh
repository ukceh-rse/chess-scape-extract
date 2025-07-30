dir=sbatch_scripts
#mkdir $dir
cp -v template.sbatch $dir
for x in {0,100000,200000,300000,400000,500000,600000}; do
    for y in {0,100000,200000,300000,400000,500000,600000,700000,800000,900000,1000000}; do
        x2=$(($x + 100000))
        y2=$(($y + 100000))
		cp -v ${dir}/template.sbatch ${dir}/chess-scape-extract_${x}_${y}.sbatch
		sed -i "s/x1/${x}/" ${dir}/chess-scape-extract_${x}_${y}.sbatch
		sed -i "s/y1/${y}/" ${dir}/chess-scape-extract_${x}_${y}.sbatch
		sed -i "s/x2/${x2}/" ${dir}/chess-scape-extract_${x}_${y}.sbatch
		sed -i "s/y2/${y2}/" ${dir}/chess-scape-extract_${x}_${y}.sbatch		
    done
done
