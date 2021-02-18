#!/bin/sh
for UPPER_BOUND_FEATURES_IN_G in 1 2 4 8 #5 10 20
do
	for N_EDGES_IN_QUERY in 2 4 6 8 10 12
	do
		for N_WRAPPERS in 2 4 8 16 32 64 128
		do
			for N_EDGES_COVERED_BY_WRAPPERS in 2 4 6 8 10 12
			do 		
				if [ $N_EDGES_COVERED_BY_WRAPPERS -le $N_EDGES_IN_QUERY ]; then
					for COVERED_FEATURES_QUERY in 1
					do
						for COVERED_FEATURES_WRAPPER in 1
						do
						  #run 1
						  ts=$(date +%s%N)
							timeout 10m java -jar build/libs/ODIN-1.0.jar 50 $UPPER_BOUND_FEATURES_IN_G $N_EDGES_IN_QUERY $N_WRAPPERS $N_EDGES_COVERED_BY_WRAPPERS $COVERED_FEATURES_QUERY $COVERED_FEATURES_WRAPPER
							tt=$((($(date +%s%N) - $ts)/1000000000))
							timeoutLimit=$((tt*5))
							sysctl -w vm.drop_caches=3 > /dev/null #free memory
							ts=$(date +%s%N) ; timeout $timeoutLimit"s" python /home/snadal/UPC/Sergi/Papers/2020/GraphQueryRewriting/evaluation/TesisDanielIzquierdo/tesis_postgrado/src/imcdsat/minicon/Main.py RW /home/snadal/UPC/Sergi/Papers/2020/GraphQueryRewriting/evaluation/miniconPython/minicon.views /home/snadal/UPC/Sergi/Papers/2020/GraphQueryRewriting/evaluation/miniconPython/minicon.query > /dev/null ; tt=$((($(date +%s%N) - $ts)/1000000)) ; echo "Minicon: $tt"
							sysctl -w vm.drop_caches=3 > /dev/null #free memory
							ts=$(date +%s%N) ; timeout $timeoutLimit"s" java -jar /home/snadal/UPC/Sergi/Papers/2020/GraphQueryRewriting/evaluation/graal/pure-rewriter-1.1.0.jar rewrite /home/snadal/UPC/Sergi/Papers/2020/GraphQueryRewriting/evaluation/graal/graal.views -q /home/snadal/UPC/Sergi/Papers/2020/GraphQueryRewriting/evaluation/graal/graal.query > /dev/null ; tt=$((($(date +%s%N) - $ts)/1000000)) ; echo "Graal: $tt"
							sysctl -w vm.drop_caches=3 > /dev/null #free memory

              #run 2
						  ts=$(date +%s%N)
							timeout 10m java -jar build/libs/ODIN-1.0.jar 50 $UPPER_BOUND_FEATURES_IN_G $N_EDGES_IN_QUERY $N_WRAPPERS $N_EDGES_COVERED_BY_WRAPPERS $COVERED_FEATURES_QUERY $COVERED_FEATURES_WRAPPER
							tt=$((($(date +%s%N) - $ts)/1000000000))
							timeoutLimit=$((tt*5))
							sysctl -w vm.drop_caches=3 > /dev/null #free memory
							ts=$(date +%s%N) ; timeout $timeoutLimit"s" python /home/snadal/UPC/Sergi/Papers/2020/GraphQueryRewriting/evaluation/TesisDanielIzquierdo/tesis_postgrado/src/imcdsat/minicon/Main.py RW /home/snadal/UPC/Sergi/Papers/2020/GraphQueryRewriting/evaluation/miniconPython/minicon.views /home/snadal/UPC/Sergi/Papers/2020/GraphQueryRewriting/evaluation/miniconPython/minicon.query > /dev/null ; tt=$((($(date +%s%N) - $ts)/1000000)) ; echo "Minicon: $tt"
							sysctl -w vm.drop_caches=3 > /dev/null #free memory
							ts=$(date +%s%N) ; timeout $timeoutLimit"s" java -jar /home/snadal/UPC/Sergi/Papers/2020/GraphQueryRewriting/evaluation/graal/pure-rewriter-1.1.0.jar rewrite /home/snadal/UPC/Sergi/Papers/2020/GraphQueryRewriting/evaluation/graal/graal.views -q /home/snadal/UPC/Sergi/Papers/2020/GraphQueryRewriting/evaluation/graal/graal.query > /dev/null ; tt=$((($(date +%s%N) - $ts)/1000000)) ; echo "Graal: $tt"
							sysctl -w vm.drop_caches=3 > /dev/null #free memory

							#run 3
						  ts=$(date +%s%N)
							timeout 10m java -jar build/libs/ODIN-1.0.jar 50 $UPPER_BOUND_FEATURES_IN_G $N_EDGES_IN_QUERY $N_WRAPPERS $N_EDGES_COVERED_BY_WRAPPERS $COVERED_FEATURES_QUERY $COVERED_FEATURES_WRAPPER
							tt=$((($(date +%s%N) - $ts)/1000000000))
							timeoutLimit=$((tt*5))
							sysctl -w vm.drop_caches=3 > /dev/null #free memory
							ts=$(date +%s%N) ; timeout $timeoutLimit"s" python /home/snadal/UPC/Sergi/Papers/2020/GraphQueryRewriting/evaluation/TesisDanielIzquierdo/tesis_postgrado/src/imcdsat/minicon/Main.py RW /home/snadal/UPC/Sergi/Papers/2020/GraphQueryRewriting/evaluation/miniconPython/minicon.views /home/snadal/UPC/Sergi/Papers/2020/GraphQueryRewriting/evaluation/miniconPython/minicon.query > /dev/null ; tt=$((($(date +%s%N) - $ts)/1000000)) ; echo "Minicon: $tt"
							sysctl -w vm.drop_caches=3 > /dev/null #free memory
							ts=$(date +%s%N) ; timeout $timeoutLimit"s" java -jar /home/snadal/UPC/Sergi/Papers/2020/GraphQueryRewriting/evaluation/graal/pure-rewriter-1.1.0.jar rewrite /home/snadal/UPC/Sergi/Papers/2020/GraphQueryRewriting/evaluation/graal/graal.views -q /home/snadal/UPC/Sergi/Papers/2020/GraphQueryRewriting/evaluation/graal/graal.query > /dev/null ; tt=$((($(date +%s%N) - $ts)/1000000)) ; echo "Graal: $tt"
							sysctl -w vm.drop_caches=3 > /dev/null #free memory

						done
					done
				fi
			done
		done
	done
done
