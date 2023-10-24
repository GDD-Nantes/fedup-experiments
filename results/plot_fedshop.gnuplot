set terminal png size 1350,300
set termoption enhanced
set output "fedshop_execution_time.png"

set pointintervalbox 0.01

set datafile separator ","

# set boxwidth 0.33
set grid ytics linestyle 0
set style fill solid 0.20 border 

set style data histograms
set style histogram clustered gap 2 title offset 0,-0.5

set xlabel "query" font ",11" offset 0,-0.6
unset xlabel
set mytics 10
set grid ytics,mytics lt -1 lc rgb "gray90", lt 0 lc rgb "gray90"

set ylabel "execution time (second)" offset 1.5
set yrange [0.001:400]
set logscale y 10

set label 1 at -0.9, 180 "120s TIMEOUT" front
timeout(x) = 120

set key at 0.5,90 right vertical font "0,9"

array colors = ["golden rod", "web-blue", "violet", "orchid4", "web-green"]


plot timeout(x) with filledcurves below y=4000 fs pattern 9 notitle lc "red", \
     newhistogram "{/=9 Single Domain Query}", for [COL=2:6] 'processed.csv' every ::0::2 using COL:xticlabels(1) title columnheader fs pattern 18 lc rgb colors[COL-1], \
     newhistogram "{/=9 Multi Domain Query}", for [COL=2:6] '' every ::3::9 skip 1 using COL:xticlabels(1) notitle fs pattern 18 lc rgb colors[COL-1], \
     newhistogram "{/=9 Cross Domain Query}", for [COL=2:6] '' every ::10::11 skip 1 using COL:xticlabels(1) notitle fs pattern 18 lc rgb colors[COL-1], \
     for [COL=7:11] 'processed.csv' every ::0::2 skip 1 using (($0)-1-2./7.+(1./7.*COL)):(column(COL-5)):COL with yerrorbars lc rgb colors[COL-6] pt 1 ps 0.5 lw 2 notitle, \
     for [COL=7:11] '' every ::0::6 skip 4 using (($0)+3-2./7.+(1./7.*COL)):(column(COL-5)):(column(COL)) with yerrorbars lc rgb colors[COL-6] pt 1 ps 0.5 lw 2 notitle, \
     for [COL=7:11] '' every ::0::1 skip 11 using (($0)+11-2./7.+(1./7.*COL)):(column(COL-5)):(column(COL)) with yerrorbars lc rgb colors[COL-6] pt 1 ps 0.5 lw 2 notitle, \
     for [COL=7:11] '' every ::0::0 using (($0)+11-2./7.+(1./7.*COL)):(column(COL-5)):(column(COL)) with yerrorbars lc rgb colors[COL-6] pt 1 ps 0.5 lw 2 notitle # does nothing but flushes for unkown bug



