rm f*;
rm *.stats;

for i in {1..20}
do
    echo "0 1 f1_"$i" "$i | ../createtable 2>/dev/null 1>>createFiles.stats;
    echo "0 1 f2_"$i" "$i | ../createtable 2>/dev/null 1>/dev/null;
done

#for i in {0..3}
#do
#    for j in {1..20}
#    do
#        echo $i" 2 f1_"$j | ../createtable 2>/dev/null 1>>"select_"$i".stats";
#    done
#done

#for i in {0..3}
#do
#    for j in {1..20}
#    do
#        echo $j;
#        for k in {1..20}
#        do
#            echo $i" 3 f1_"$j" f2_"$k | ../createtable 2>/dev/null 1>>"join_"$i".stats";
#        done
#    done
#done

for i in {0..3}
do
    for j in {0..20}
    do
        echo $i" 3 f1_"$j" f2_"$j | ../createtable 2>/dev/null 1>>"join_"$i".stats";
    done
done
