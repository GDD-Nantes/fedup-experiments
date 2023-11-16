set terminal png size 2100,600 font ",20"
set termoption enhanced
set output "largerdfbench_execution_time.png"
set termoption enhanced

set pointintervalbox 0.01

set datafile separator ","

set boxwidth 0.9
set grid ytics linestyle 0
set style fill solid 0.20 border

set style data histograms
set style histogram clustered gap 2 title offset 0,-0.8

set xlabel "query" font ",11" offset 0,-0.6
unset xlabel
set mytics 10
set grid ytics,mytics lt -1 lc rgb "gray90", lt 0 lc rgb "gray90"

set ylabel "execution time (second)" offset 2.3
set yrange [0.001:4000]
set logscale y 10

set label 1 at -0.9, 1800 "1200s TIMEOUT" front font ",20"
timeout(x) = 1200

set key at 1.5,900 right vertical font "0,14"

array colors = ["orange", "web-blue", "violet", "orchid4", "web-green"]

set bmargin 3

plot timeout(x) with filledcurves below y=4000 fs pattern 9 notitle lc "light-red", \
     newhistogram "{/=18 Simple Query}", for [COL=2:6] 'processed.csv' every ::0::13 using COL:xticlabels(1) title columnheader fs pattern 11 fill transparent lc rgb colors[COL-1], \
     newhistogram "{/=18 Complex Query}", for [COL=2:6] '' every ::0::9 skip 15 using COL:xticlabels(1) notitle fs pattern 11 fill transparent lc rgb colors[COL-1], \
     for [COL=7:11] '' every ::0::0 using (($0)+11-2./7.+(1./7.*COL)):(column(COL-5)):(column(COL)) with yerrorbars lc rgb colors[COL-6] pt 1 ps 0.5 lw 2 notitle # does nothing but flushes for unkown bug

##     for [COL=7:11] 'processed.csv' every ::0::14 skip 1 using (($0)-1-2./7.+(1./7.*COL)):(column(COL-5)):COL with yerrorbars lc rgb colors[COL-6] pt 1 ps 0.5 lw 2 notitle, 
##      for [COL=7:11] '' every ::0::9 skip 15  using (($0)+3-2./7.+(1./7.*COL)):(column(COL-5)):(column(COL)) with yerrorbars lc rgb colors[COL-6] pt 1 ps 0.5 lw 2 notitle, \




