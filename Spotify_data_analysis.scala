val spotifyRdd = sc.textFile("*.csv")   //Reading the textFile

val head = spotifyRdd.first            // Getting the header names
val spotify_data = spotifyRdd.filter(r => r != head) // removing the headers from the data
spotify_data.toDF.show(10)                // displaying the contents

/*+--------------------+
|               value|
+--------------------+
|35iwgR4jXetI318WE...|
|021ht4sdgPcrDgSk7...|
|07A5yehtSnoedViJA...|
|08FmqUhxtyLTn6pAh...|
|08y9GfoqCWfOGsKdw...|
|0BRXJHRNGQ3W4v9fr...|
|0Dd9ImXtAtGwsmsAD...|
|0IA0Hju8CAgYfV1hw...|
|0IgI1UCz84pYeVetn...|
|0JV4iqw2lSKJaHBQZ...|
+--------------------+
*/

spotify_data.count                        // No.of rows in the data

println("********************************  Number of rows in the data  :  " + spotify_data.count)  
/*
********************************  Number of rows in the data  :  2548767
*/

// Splitting the data into its components and cleaning it 

val spot_data = spotify_data.map{row => val r = row.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)
            val(id,nm,pp,tm,exp) = (r(0),r(1),r(2).toInt,r(3).toInt,r(4).toInt)
            val(ar,ar_id,dt,dn,eg) = (r(5),r(r.length-14),r(r.length-13),r(r.length-12).toFloat,r(r.length-11).toFloat)
            val(key,ld,md,sp,ac) = (r(r.length-10).toInt,r(r.length-9).toFloat,r(r.length-8).toInt,r(r.length-7).toFloat,r(r.length-6).toFloat)
            val(in,lv,vl,tmp,sgn) = (r(r.length-5).toFloat,r(r.length-4).toFloat,r(r.length-3).toFloat,r(r.length-2).toFloat,r(r.length-1).toInt)
            (id,nm,pp,tm,exp,ar,ar_id,dt,dn,eg,key,ld,md,sp,ac,in,lv,vl,tmp,sgn)}

// Conerting milisecs to minutes and dates to year
val yearly = spot_data.map{h => val st = h._8.split("-")
    (h._1,h._2,h._3,(h._4*0.00167).round/100.toDouble,h._5,h._6,h._7,st(0),h._9,h._10,h._11,h._12,h._13,h._14,h._15,h._16,h._17,h._18,h._19,h._20)}

println("******************************** CLEANED DATA ********************************")

yearly.toDF("id","name","popul","dura","cont","artists","ID_arts","DOR","dance","energy","key","loudness","mode","speech","acoustics","instr","liveness","valn","tempo","time_sng").show(10)
/*

+--------------------+--------------------+-----+----+----+-------------------+--------------------+----+-----+------+---+--------+----+------+---------+-------+--------+------+-------+--------+
|                  id|                name|popul|dura|cont|            artists|             ID_arts| DOR|dance|energy|key|loudness|mode|speech|acoustics|  instr|liveness|  valn|  tempo|time_sng|
+--------------------+--------------------+-----+----+----+-------------------+--------------------+----+-----+------+---+--------+----+------+---------+-------+--------+------+-------+--------+
|35iwgR4jXetI318WE...|               Carve|    6|2.12|   0|            ['Uli']|['45tIt06XoI0Iio4...|1922|0.645| 0.445|  0| -13.338|   1| 0.451|    0.674|  0.744|   0.151| 0.127|104.851|       3|
|021ht4sdgPcrDgSk7...|Capítulo 2.16 - B...|    0|1.64|   0|['Fernando Pessoa']|['14jtPCOoNZwquk5...|1922|0.695| 0.263|  0| -22.136|   1| 0.957|    0.797|    0.0|   0.148| 0.655|102.009|       1|
|07A5yehtSnoedViJA...|Vivo para Querert...|    0|3.03|   0|['Ignacio Corsini']|['5LiOoJbxVSAMkBS...|1922|0.434| 0.177|  1|  -21.18|   1|0.0512|    0.994| 0.0218|   0.212| 0.457|130.418|       5|
|08FmqUhxtyLTn6pAh...|El Prisionero - R...|    0|2.95|   0|['Ignacio Corsini']|['5LiOoJbxVSAMkBS...|1922|0.321|0.0946|  7| -27.961|   1|0.0504|    0.995|  0.918|   0.104| 0.397| 169.98|       3|
|08y9GfoqCWfOGsKdw...| Lady of the Evening|    0|2.72|   0|    ['Dick Haymes']|['3BiJGZsyX9sJchT...|1922|0.402| 0.158|  3|   -16.9|   0| 0.039|    0.989|   0.13|   0.311| 0.196| 103.22|       4|
|0BRXJHRNGQ3W4v9fr...|           Ave Maria|    0|2.99|   0|    ['Dick Haymes']|['3BiJGZsyX9sJchT...|1922|0.227| 0.261|  5| -12.343|   1|0.0382|    0.994|  0.247|  0.0977|0.0539|118.891|       4|
|0Dd9ImXtAtGwsmsAD...|      La Butte Rouge|    0|2.25|   0|  ['Francis Marty']|['2nuMRGzeJ5jJEKl...|1922| 0.51| 0.355|  4| -12.833|   1| 0.124|    0.965|    0.0|   0.155| 0.727| 85.754|       5|
|0IA0Hju8CAgYfV1hw...|             La Java|    0| 2.7|   0|    ['Mistinguett']|['4AxgXfD7ISvJSTO...|1922|0.563| 0.184|  4| -13.757|   1|0.0512|    0.993|1.55E-5|   0.325| 0.654|133.088|       3|
|0IgI1UCz84pYeVetn...|  Old Fashioned Girl|    0|5.18|   0|    ['Greg Fieler']|['5nWlsH5RDgFuRAi...|1922|0.488| 0.475|  0| -16.222|   0|0.0399|     0.62|0.00645|   0.107| 0.544|139.952|       4|
|0JV4iqw2lSKJaHBQZ...|Martín Fierro - R...|    0|3.03|   0|['Ignacio Corsini']|['5LiOoJbxVSAMkBS...|1922|0.548|0.0391|  6| -23.228|   1| 0.153|    0.996|  0.933|   0.148| 0.612| 75.595|       3|
+--------------------+--------------------+-----+----+----+-------------------+--------------------+----+-----+------+---+--------+----+------+---------+-------+--------+------+-------+--------+
only showing top 10 rows

*/

//Finding the unique artists
val unique_artists = yearly.map(r => (r._6,1)).reduceByKey(_+_)
println("******************************** UNIQUE ARTISTS ********************************")
unique_artists.toDF("UNIQUE ARTISTS","NO. of songs").show

/*
+--------------------+------------+
|      UNIQUE ARTISTS|NO. of songs|
+--------------------+------------+
|     ['Nia Daniaty']|          12|
|"['Igor Stravinsk...|          10|
|             ['FMK']|           2|
|"['Natiruts', 'Cl...|           2|
|"['Zeenat Begum',...|          10|
|          ['Slayer']|         308|
|"['Luke Bond', 'O...|           9|
|"['Aaron Copland'...|           1|
|     ['The Osmonds']|          48|
|    ['Nahuel Virus']|           1|
|          ['Scubba']|           1|
|"['Uffe Steen', '...|           2|
|['Niney The Obser...|           9|
|"['Vicetone', 'Co...|           9|
|"['Sergei Prokofi...|           7|
|"['Mumtaz', 'Asho...|          10|
|"['Roza Eskenazi'...|          10|
|['Kings of Conven...|          22|
|"['Stabb', 'Gover...|          10|
|        ['Promises']|           7|
+--------------------+------------+
only showing top 20 rows
*/


//Filtering artists with songs greater than 500 
val artists_most_songs = unique_artists.filter(h => h._2 >= 500)
val sort_artists = artists_most_songs.sortBy(_._2 ,false)  // Sorting in descending order

println("******************************* ARTISTS - NO OF SONGS ******************************")
sort_artists.toDF("ARTISTS","NUMBER OF SONGS").show
/*
+--------------------+---------------+
|             ARTISTS|NUMBER OF SONGS|
+--------------------+---------------+
|['Francisco Canaro']|           8319|
|['Tadeusz Dolega ...|           6482|
| ['Ignacio Corsini']|           5276|
|  ['Billie Holiday']|           4138|
| ['Ella Fitzgerald']|           4076|
|   ['Frank Sinatra']|           4038|
|"['Georgette Heye...|           3823|
|"['Francisco Cana...|           3648|
|  ['Janusz Korczak']|           3550|
|    ['Die drei ???']|           3351|
|['Arthur Conan Do...|           3263|
|   ['Elvis Presley']|           3222|
|     ['The Beatles']|           3171|
|['The Rolling Sto...|           2842|
|      ['Lead Belly']|           2841|
|     ['Dean Martin']|           2476|
|           ['Queen']|           2294|
|     ['David Bowie']|           2208|
|    ['???? ???????']|           2160|
|"['Francisco Cana...|           2154|
+--------------------+---------------+
only showing top 20 rows
*/


println("************************************************************** COMPARISON THE FOUR DECADES - 80'S , 90'S , early-2K, 2K ******************************************************")


//a) Filtering the songs released between 1980 - 1990 
val eighties = yearly.filter(h => (h._8 >= "1980" && h._8 < "1990"))
println("******************************** 80's DECADE  ********************************")
eighties.toDF("id","name","popul","dura","cont","artists","ID_arts","DOR","dance","energy","key","loudness","mode","speech","acoustics","instr","liveness","valn","tempo","time_sng").show(10)
/*
+--------------------+--------------------+-----+----+----+--------------------+--------------------+----+-----+------+---+--------+----+------+---------+-------+--------+-----+-------+--------+
|                  id|                name|popul|dura|cont|             artists|             ID_arts| DOR|dance|energy|key|loudness|mode|speech|acoustics|  instr|liveness| valn|  tempo|time_sng|
+--------------------+--------------------+-----+----+----+--------------------+--------------------+----+-----+------+---+--------+----+------+---------+-------+--------+-----+-------+--------+
|08mG3Y1vljYA6bvDt...|       Back In Black|   85|4.27|   0|           ['AC/DC']|['711MCceyCBcFnzj...|1980| 0.31|   0.7|  9|  -5.678|   1| 0.047|    0.011|0.00965|  0.0828|0.763|188.386|       4|
|5vdp5UmvTsnMEMESI...|Another One Bites...|   83|3.58|   0|           ['Queen']|['1dfeR4HaWDbWqFH...|1980|0.933| 0.528|  5|  -6.472|   0| 0.161|    0.112|  0.312|   0.163|0.754|109.967|       4|
|2SiXAy7TuUkycRVbb...|You Shook Me All ...|   81|3.51|   0|           ['AC/DC']|['711MCceyCBcFnzj...|1980|0.532| 0.767|  7|  -5.509|   1|0.0574|  0.00287|5.13E-4|    0.39|0.755|127.361|       4|
|4o6BgsqLIBViaGVbx...|You Make My Dream...|   79|3.18|   0|['Daryl Hall & Jo...|['77tT1kLj6mCWtFN...|1980|0.751| 0.501|  5| -12.151|   1|0.0551|    0.234|  0.112|  0.0467|0.902|167.057|       4|
|5O4erNlJ74PIF6kGo...|  Could You Be Loved|   79|3.96|   0|['Bob Marley & Th...|['2QsynagSdAqZj3U...|1980|0.916|  0.72|  0|  -8.548|   1|   0.1|     0.36| 1.6E-4|  0.0958| 0.76|103.312|       4|
|4w3tQBXhn5345eUXD...|              9 to 5|   78|2.71|   0|    ['Dolly Parton']|['32vWCbZh0xZ4o9g...|1980|0.554| 0.783|  6|  -5.852|   1|0.0457|    0.416|1.54E-6|   0.631|0.813| 105.39|       4|
|6xdLJrVj4vIXwhuG8...|Crazy Little Thin...|   76|2.73|   0|           ['Queen']|['1dfeR4HaWDbWqFH...|1980|0.599| 0.761|  0|  -6.887|   1|0.0421|    0.713|4.74E-6|   0.349|0.712| 77.015|       4|
|6EPRKhUOdiFSQwGBR...|       Ace of Spades|   76|2.79|   0|       ['Motörhead']|['1DFr97A9HnbV3SK...|1980|0.329| 0.974|  3|   -8.77|   0| 0.135|  8.52E-4|1.18E-4|  0.0904|0.234|140.862|       4|
|5fdNHVZHbWB1AaXk4...|  Just the Two of Us|   75|7.41|   0|"['Grover Washing...|['05YVYeV4HxYp5rr...|1980|0.743|  0.43| 10| -14.575|   0|0.0906|    0.445|  0.363|  0.0455|0.593| 95.406|       4|
|69QHm3pustz01CJRw...|         Hells Bells|   75|5.22|   0|           ['AC/DC']|['711MCceyCBcFnzj...|1980|0.389| 0.873|  4|  -4.768|   0|0.0475|  0.00532| 0.0055|   0.273|0.303|106.767|       4|
+--------------------+--------------------+-----+----+----+--------------------+--------------------+----+-----+------+---+--------+----+------+---------+-------+--------+-----+-------+--------+
only showing top 10 rows
*/
println("********************************  Number of songs released in 80's  :  " + eighties.count )
/*
********************************  Number of songs released in 80's  :  100678
*/


//b) Filtering the songs released between 1990 - 2000 
val nineties = yearly.filter(h => (h._8 >= "1990" && h._8 < "2000"))
println("******************************** 90's DECADE  ********************************")
nineties.toDF("id","name","popul","dura","cont","artists","ID_arts","DOR","dance","energy","key","loudness","mode","speech","acoustics","instr","liveness","valn","tempo","time_sng").show(10)
/*
+--------------------+--------------------+-----+----+----+--------------------+--------------------+----+-----+------+---+--------+----+------+---------+-------+--------+-----+-------+--------+
|                  id|                name|popul|dura|cont|             artists|             ID_arts| DOR|dance|energy|key|loudness|mode|speech|acoustics|  instr|liveness| valn|  tempo|time_sng|
+--------------------+--------------------+-----+----+----+--------------------+--------------------+----+-----+------+---+--------+----+------+---------+-------+--------+-----+-------+--------+
|67BBzbF6t4ejG7HwV...|"Love For Sale - ...|   18|4.87|   0|['Buddy Rich Big ...|['5cyeaYQ80Wkqvxx...|1997|0.409| 0.839|  5|  -7.695|   0|   0.2|    0.445|  0.235|   0.958|0.557|112.624|       4|
|4Gh4b2Rc456d8rFKz...|           Jet Black|   16|2.52|   0|    ['The Drifters']|['1FqqOl9itIUpXr4...|1991|0.707| 0.435|  9| -10.206|   0|0.0478|    0.434| 0.0247|  0.0947|0.647|123.647|       4|
|3cqXzWoraiskCEUY8...|             Shotgun|   15|2.13|   0|     ['The Shadows']|['03hfAxVdAWj7kxD...|1991|0.589|  0.73|  2|  -7.155|   1|0.0362|    0.355|  0.531|   0.101|0.951|129.083|       4|
|5D5xkqCaq3CwYjeZv...|   Kuningatar - Live|   15|4.17|   0|          ['Hector']|['5UaXeIdkgIbWPwn...|1992|0.398| 0.517|  0| -12.538|   1|0.0605|   0.0264|    0.0|   0.822|0.548|171.774|       3|
|5FHwYCRj84i9S7oX7...|           Blue Star|   13|2.77|   0|     ['The Shadows']|['03hfAxVdAWj7kxD...|1991| 0.64| 0.261| 11| -13.356|   0|0.0315|    0.435|1.64E-4|   0.284|0.582|112.896|       4|
|7F92iC8lExz1h5XNX...|Theme from a Fill...|    8|2.41|   0|     ['The Shadows']|['03hfAxVdAWj7kxD...|1991|0.697| 0.493|  9| -11.505|   1|0.0691|    0.817|  0.863|   0.151|0.861|130.111|       4|
|43bsnLYIUDp2QGn8J...| Palkkasoturi - Live|    9|3.38|   0|          ['Hector']|['5UaXeIdkgIbWPwn...|1992|0.365| 0.306|  6| -16.908|   0|0.0382|      0.5|2.29E-6|    0.81| 0.27| 81.785|       4|
|5qmRAnvQVikJd9RLm...|            Midnight|    5|2.53|   0|     ['The Shadows']|['03hfAxVdAWj7kxD...|1991|0.546| 0.448|  9|   -7.73|   0|0.0274|    0.368| 0.0404|  0.0535|0.511|114.702|       3|
|4HGicFggShhJa1tVS...|             Big Boy|    6|2.18|   0|     ['The Shadows']|['03hfAxVdAWj7kxD...|1991|0.605| 0.798|  9| -10.695|   1|0.0281|    0.745|  0.921|   0.116|0.962|126.548|       4|
|6jgQhqvgmDBBeZAzr...|  It's a Man's World|    5|2.12|   0|     ['The Shadows']|['03hfAxVdAWj7kxD...|1991| 0.55|  0.75|  5| -10.985|   1|0.0271|   0.0701|  0.751|   0.226|0.965|141.748|       4|
+--------------------+--------------------+-----+----+----+--------------------+--------------------+----+-----+------+---+--------+----+------+---------+-------+--------+-----+-------+--------+
only showing top 10 rows
*/
println("********************************  Number of songs released in 90's  :  " + nineties.count )
/*
********************************  Number of songs released in 90's  :  98487
*/


//c) Filtering the songs released between 2000 - 2010
val early_twok = yearly.filter(h => (h._8 >= "2000" && h._8 < "2010")) 
println("******************************** early 2k DECADE  ********************************")
early_twok.toDF("id","name","popul","dura","cont","artists","ID_arts","DOR","dance","energy","key","loudness","mode","speech","acoustics","instr","liveness","valn","tempo","time_sng").show(10)
/*
+--------------------+--------------------+-----+----+----+--------------------+--------------------+----+-----+------+---+--------+----+------+---------+-------+--------+-----+-------+--------+
|                  id|                name|popul|dura|cont|             artists|             ID_arts| DOR|dance|energy|key|loudness|mode|speech|acoustics|  instr|liveness| valn|  tempo|time_sng|
+--------------------+--------------------+-----+----+----+--------------------+--------------------+----+-----+------+---+--------+----+------+---------+-------+--------+-----+-------+--------+
|6catF1lDhNTjjGa2G...|You'll Never Walk...|   56|2.68|   0|['Gerry & The Pac...|['3UmBeGyNwr4iDWi...|2008|0.484| 0.265|  0| -11.101|   1|0.0322|    0.394|    0.0|   0.149|0.285|113.564|       3|
|4aSw1QJIMwYSoDEgz...|Ferry Cross the M...|   40|2.37|   0|['Gerry & The Pac...|['3UmBeGyNwr4iDWi...|2008|0.405| 0.365|  6| -10.226|   0|0.0289|    0.255|4.68E-6|   0.163|0.588|104.536|       4|
|0ZMMtH875IR2TfkyC...|Don't Let the Sun...|   35|2.62|   0|['Gerry & The Pac...|['3UmBeGyNwr4iDWi...|2008|0.477| 0.352|  1| -14.165|   1|  0.03|    0.406|    0.0|   0.122|0.478|106.773|       4|
|7LfvdUcwrrMKVh8WP...|How Do You Do It?...|   29|1.92|   0|['Gerry & The Pac...|['3UmBeGyNwr4iDWi...|2008|0.617| 0.711|  9|  -6.433|   1|0.0297|     0.36|1.59E-6|  0.0841|0.963|142.266|       4|
|25qyOLQyX7bsceW3U...|   Hello Little Girl|   25|1.92|   0|['Gerry & The Pac...|['3UmBeGyNwr4iDWi...|2008|0.409| 0.639|  6|  -7.628|   1|0.0302|    0.288|    0.0|   0.343|0.928|145.903|       4|
|4LDaBsMVANIqGcD4r...|Don't Let the Sun...|   23|2.65|   0|['Gerry & The Pac...|['3UmBeGyNwr4iDWi...|2008|0.459| 0.344|  1| -12.205|   1|0.0301|    0.764|5.94E-4|   0.111|0.487|106.479|       4|
|1dZ8WUAsipdyBLUQk...|Ferry Cross the M...|   23|2.41|   0|['Gerry & The Pac...|['3UmBeGyNwr4iDWi...|2008|0.399| 0.296|  6| -12.364|   0|0.0294|      0.5|    0.0|  0.0719|0.658|107.098|       4|
|1hangrBJi1Tshxcm7...|I'll Be There - F...|   22| 3.3|   0|['Gerry & The Pac...|['3UmBeGyNwr4iDWi...|2008|0.609|  0.29|  7| -11.226|   1|0.0342|    0.696|2.74E-5|   0.108|0.487|122.434|       4|
|7HMeul83qsxs8oK1Q...|    I Like It (Main)|   22|2.27|   0|['Gerry & The Pac...|['3UmBeGyNwr4iDWi...|2008| 0.55| 0.552|  7| -10.334|   1|0.0344|     0.33|    0.0|  0.0907|0.964|147.715|       4|
|3j60Gwb3quixJNGZg...|The End of the Ra...|   22|2.24|   0|['Gerry & The Pac...|['3UmBeGyNwr4iDWi...|2008|  0.6| 0.374|  2| -14.046|   1|0.0264|    0.773|1.69E-4|   0.112|0.556| 95.023|       3|
+--------------------+--------------------+-----+----+----+--------------------+--------------------+----+-----+------+---+--------+----+------+---------+-------+--------+-----+-------+--------+
only showing top 10 rows
*/
println("********************************  Number of songs released in early 2k  :  " + early_twok.count )
/*
********************************  Number of songs released in early 2k  :  188367
*/

//d) Filtering the songs released between 2010 - Present
val twok = yearly.filter(h => (h._8 >= "2010")) 
println("******************************** 2k DECADE  ********************************")
twok.toDF("id","name","popul","dura","cont","artists","ID_arts","DOR","dance","energy","key","loudness","mode","speech","acoustics","instr","liveness","valn","tempo","time_sng").show(10)
/*
+--------------------+--------------------+-----+----+----+--------------------+--------------------+----+-----+------+---+--------+----+------+---------+-------+--------+-----+-------+--------+
|                  id|                name|popul|dura|cont|             artists|             ID_arts| DOR|dance|energy|key|loudness|mode|speech|acoustics|  instr|liveness| valn|  tempo|time_sng|
+--------------------+--------------------+-----+----+----+--------------------+--------------------+----+-----+------+---+--------+----+------+---------+-------+--------+-----+-------+--------+
|1a3xGSvqe2gK14WxL...|"Symphony No. 4 i...|    0|9.62|   0|"['Anton Bruckner...|"['2bM3j1JQWBkmzu...|2020|0.258| 0.263|  1| -13.462|   1|0.0482|    0.957|  0.698|   0.272|0.111| 128.94|       4|
|6Pkt6qVikqPBt9bEQ...|  A Lover's Concerto|   41|2.66|   0|        ['The Toys']|['6lH5PpuiMa5Spfj...|2020|0.671| 0.867|  2|  -2.706|   1|0.0571|    0.436|    0.0|   0.139|0.839|120.689|       4|
|1hx7X9cMXHWJjknb9...|"The September Of...|   26|3.13|   0|   ['Frank Sinatra']|['1Mxqyy3pSjf8kZZ...|2018|0.319| 0.201|  7| -17.796|   1|0.0623|    0.887|    0.0|   0.904|0.239|117.153|       3|
|19oquvXf3bc65GSqt...|"It Was A Very Go...|   26|3.95|   0|   ['Frank Sinatra']|['1Mxqyy3pSjf8kZZ...|2018|0.269| 0.129|  7| -18.168|   0|0.0576|    0.938|4.87E-6|   0.683| 0.16| 82.332|       3|
|55qyghODi24yaDgKB...|"The Circle Game ...|   18|5.23|   0|   ['Joni Mitchell']|['5hW4L92KnC6dX9t...|2020|0.644| 0.212| 11| -14.118|   1|0.0347|    0.881|2.22E-5|   0.798|0.441|117.072|       3|
|00xemFYjQNRpOlPhV...|"Urge For Going -...|   18|4.93|   0|   ['Joni Mitchell']|['5hW4L92KnC6dX9t...|2020|0.627| 0.184|  1| -15.533|   1| 0.045|    0.955|1.62E-4|  0.0986|0.299|115.864|       4|
|26g4FBGTB9YEj7q4H...|"Brandy Eyes - Li...|   17|2.47|   0|   ['Joni Mitchell']|['5hW4L92KnC6dX9t...|2020|0.442| 0.399|  6| -12.661|   1| 0.078|     0.93|4.99E-4|   0.912|0.554|121.662|       4|
|05sxkljafFBW2vEnV...|"Intro To Urge Fo...|   17|1.07|   0|   ['Joni Mitchell']|['5hW4L92KnC6dX9t...|2020| 0.57| 0.176|  6| -22.676|   0| 0.299|    0.949|    0.0|   0.147|0.348|135.687|       1|
|2lm5FQJRHvc3rUN5Y...|"What's The Story...|   17|3.06|   0|   ['Joni Mitchell']|['5hW4L92KnC6dX9t...|2020|0.581| 0.331|  6| -14.087|   1| 0.243|    0.888| 1.5E-5|   0.147|0.642| 88.303|       4|
|0GohHD8bn4lP83agk...|"Eastern Rain - L...|   17| 3.9|   0|   ['Joni Mitchell']|['5hW4L92KnC6dX9t...|2020|0.598| 0.212|  6| -15.078|   0|0.0406|    0.932|2.32E-5|   0.692|0.172|107.183|       4|
+--------------------+--------------------+-----+----+----+--------------------+--------------------+----+-----+------+---+--------+----+------+---------+-------+--------+-----+-------+--------+
only showing top 10 rows
*/
println("********************************  Number of songs released in  2k  :  " + early_twok.count )
/*
********************************  Number of songs released in  2k  :  381568
*/

println("***************************************** HOW POPULARITY OF ARTISTS AFFECT THE POPULARITY OF SONGS ?  *************************************")



println("----------------------------------------------- 80 --------------------------------------------------------")
//Finding POPULAR SONGS in 80s

// Finding the songs with popularity greater than 70 
val pop_songs_80 = eighties.filter{h => h._3 >= 70}
pop_songs.count // 2786
// Finding the popular artists who gave the popular songs
val pop_songs_per_arts_80s = pop_songs_80.map(h => (h._6,1)).reduceByKey(_+_)
pop_songs_per_arts_80s.count //181
pop_songs_per_arts_80s.sortBy(_._2,false).toDF("ARTISTS","NUMBER OF HITS").show(20) // Popular number of songs per artists in 80s
/*
+--------------------+--------------+
|             ARTISTS|NUMBER OF HITS|
+--------------------+--------------+
| ['Michael Jackson']|            70|
|"[""Guns N' Roses...|            60|
|        ['Bon Jovi']|            60|
|['Bruce Springste...|            50|
|           ['Queen']|            50|
|              ['U2']|            40|
|           ['AC/DC']|            40|
|    ['Phil Collins']|            40|
|   ['Fleetwood Mac']|            40|
|     ['Mötley Crüe']|            40|
|    ['Dire Straits']|            36|
|      ['Elton John']|            35|
|       ['Metallica']|            30|
|         ['Journey']|            30|
|       ['Foreigner']|            30|
|    ['Cyndi Lauper']|            30|
|      ['The Police']|            30|
|   ['Lionel Richie']|            30|
|      ['The Smiths']|            30|
|       ['Van Halen']|            30|
+--------------------+--------------+
only showing top 20 rows
*/

// Percentage of Popular songs in 80 decade
val perc_popartists_80s = pop_songs_80.count/eighties.count.toDouble *100
println("********************************  % of popular songs in 80s  :  " + perc_popartists_80s )
/*
********************************  % of popular songs in 80s  :  2.767238125509049
*/

// Finding POPULAR AND UNPOPULAR ARTISTS

// Finding POPULAR Artists
val unique_artists_80s = eighties.map(h => (h._6,1)).reduceByKey(_+_) // List of unique Artists from 1980s - key value pair
val mostsongs_artists_80 = unique_artists_80s.filter(h => h._2 >= 300)    // Filtering the Artists with the most number of songs
println("******************************** NO. of artists with more than 300 songs: " + mostsongs_artists_80.count) //30

val freq80_key = mostsongs_artists_80.map(h => (h._1))                // Collecting just the Artists with the most number of songs in 80s (i.e. Key from KVpair)
val Setsortfreq_80s = freq80_key.collect.toSet                          // Creating a set of Artists with the most number of songs in 80s
val data_freq80s = eighties.filter{h => Setsortfreq_80s.contains(h._6)}   // Filtering the data of Artists with the most number of songs in 80s
val pop_songs_80s = data_freq80s.filter{h => h._3 >= 70} //The number of songs which are popular and belong to an artist with no.of songs>300 

val total_pop_songs_80s = mostsongs_artists_80.map(_._2).reduce(_+_)  // The total number of songs of the artists with no.of songs>300 = 16400
val Frac_pop_80s = pop_songs_80s.count/total_pop_songs_80s.toDouble *100  // Finding the fraction og popular songs from Popular Artists
println("********************************  Fraction of popular songs from the popular artists : " + Frac_pop_80s )  
/*
 ********************************  Fraction of popular songs from the popular artists : 4.548780487804878
*/

// Finding UNPOPULAR Artists
val leastsongs_artists_80 = unique_artists_80s.filter(h => h._2 <= 100)   //Filtering the Artists with less number of songs
println("******************************** NO. of artists with less than 100 songs: " + leastsongs_artists_80.count) //4716

val freq80s_key_least = leastsongs_artists_80.map(h => (h._1))                // Collecting just the Artists with the least number of songs in 80s (i.e. Key from KVpair)
val Setsortfreq_least_80s = freq80s_key_least.collect.toSet                          // Creating a set of Artists with the least number of songs in 80s 
val data_freq_least80s = eighties.filter{h => Setsortfreq_least_80s.contains(h._6)}  // Filtering the data of Artists with the least number of songs in 80s
val pop_least_80s = data_freq_least80s.filter{h => h._3 >= 70}       //The number of songs which are popular and belong to an artist with no.of songs<100 
val total_least_songs_80s = leastsongs_artists_80.map(_._2).reduce(_+_)  // The total number of songs of the artists with no.of songs<100 is 57498
val Frac_unpop_80s = pop_least_80s.count/total_least_songs_80s.toDouble *100
println("********************************  Fraction of popular songs from the unpopular artists : " + Frac_unpop_80s )  
/*
********************************  Fraction of popular songs from the unpopular artists : 1.6104907996799886
*/

/*
CONCLUSION - 80'S - The Percentage of popular songs from popular and unpopular artists are close
but however the popularity of the artists have slightly influenced the popularity of songs in the 80s
*/


println("------------------------------------------------ 90 ------------------------------------------------------------")
//Finding POPULAR SONGS in 90s

// Finding the songs with popularity greater than 70 
val pop_songs_90 = nineties.filter{h => h._3 >= 70}
pop_songs_90.count // 3597
// Finding the popular artists who gave the popular songs
val pop_songs_per_arts_90s = pop_songs_90.map(h => (h._6,1)).reduceByKey(_+_)
pop_songs_per_arts_90s.count //258
pop_songs_per_arts_90s.sortBy(_._2,false).toDF("ARTISTS","NUMBER OF HITS").show(20) // Popular number of songs per artists in 90s
/*
+--------------------+--------------+
|             ARTISTS|NUMBER OF HITS|
+--------------------+--------------+
|         ['Nirvana']|            66|
|       ['Pearl Jam']|            60|
|     ['Luis Miguel']|            52|
|       ['Metallica']|            50|
|    ['Mariah Carey']|            50|
|       ['Radiohead']|            50|
|"[""Guns N' Roses...|            50|
|['Red Hot Chili P...|            50|
|            ['2Pac']|            47|
| ['The Cranberries']|            40|
|['The Notorious B...|            40|
| ['Alice In Chains']|            40|
|       ['Aerosmith']|            40|
|            ['Maná']|            40|
|       ['Green Day']|            38|
| ['Michael Jackson']|            30|
|   ['Lenny Kravitz']|            30|
|         ['Shakira']|            30|
| ['Whitney Houston']|            30|
|       ['blink-182']|            30|
+--------------------+--------------+
only showing top 20 rows
*/


// Percentage of Popular songs in 90 decade
val perc_popartists_90s = pop_songs_90.count/nineties.count.toDouble *100
println("********************************  % of popular songs in 90s  :  " + perc_popartists_90s )
/*
********************************  % of popular songs in 90s  :  3.652258673733589
*/

// Finding POPULAR AND UNPOPULAR ARTISTS

// Finding POPULAR Artists
val unique_artists_90s = nineties.map(h => (h._6,1)).reduceByKey(_+_)   // List of unique Artists from 1990s - key value pair
val mostsongs_artists_90 = unique_artists_90s.filter(h => h._2 >= 300)    // Filtering the Artists with the most number of songs
println("******************************** NO. of artists with more than 300 songs: " + mostsongs_artists_90.count) //30
/*
******************************** NO. of artists with more than 300 songs: 20
*/

val freq90_key = mostsongs_artists_90.map(h => (h._1))                // Collecting just the Artists with the most number of songs in 90s (i.e. Key from KVpair)
val Setsortfreq_90s = freq90_key.collect.toSet                          // Creating a set of Artists with the most number of songs in 90s
val data_freq90s = nineties.filter{h => Setsortfreq_90s.contains(h._6)}   // Filtering the data of Artists with the most number of songs in 90s
val pop_songs_90s = data_freq90s.filter{h => h._3 >= 70}


val total_pop_songs_90s = mostsongs_artists_90.map(_._2).reduce(_+_)  //The number of songs which are popular and belong to an artist with no.of songs>300 = 8802
val Frac_pop_90s = pop_songs_90s.count/total_pop_songs_90s.toDouble *100 //Fraction of popular songs from the popular artists =  Frac_90s =
 
println("********************************  Fraction of popular songs from the popular artists : " + Frac_pop_90s )  
/*
********************************  Fraction of popular songs from the popular artists : 6.748466257668712
*/

// Finding UNPOPULAR Artists
val leastsongs_artists_90 = unique_artists_90s.filter(h => h._2 <= 100)   //Filtering the Artists with less number of songs
println("******************************** NO. of artists with less than 100 songs: "+ leastsongs_artists_90.count)
/*
******************************** NO. of artists with less than 100 songs: 7112
*/

val freq90s_key_least = leastsongs_artists_90.map(h => (h._1))                // Collecting just the Artists with the most number of songs in 90s (i.e. Key from KVpair)
val Setsortfreq_least_90s = freq90s_key_least.collect.toSet                          // Creating a set of Artists with the most number of songs in 90s 
val data_freq_least90s = nineties.filter{h => Setsortfreq_least_90s.contains(h._6)} 
val pop_least_90s = data_freq_least90s.filter{h => h._3 >= 70}  // the number of songs which are popular and belong to an artist with no.of songs<100 = 57498
val total_least_songs_90s = leastsongs_artists.map(_._2).reduce(_+_) // The total number of songs of the artists with no.of songs<100 is 
val Frac_unpop_90s = pop_least_90s.count/total_least_songs_90s.toDouble *100

println("********************************  Fraction of popular songs from the unpopular artists : " + Frac_unpop_90s )  
/*
********************************  Fraction of popular songs from the unpopular artists : 3.0070611151692233
*/

/*
CONCLUSION - 90'S - The Percentage of popular songs from popular and unpopular artists are close
but however the popularity of the artists have slightly influenced the popularity of songs in the 90s
*/


println("------------------------------------------------ early 2k ------------------------------------------------------------")
//Finding POPULAR SONGS in early 2k

// Finding the songs with popularity greater than 70 
val pop_songs_2k = early_twok.filter{h => h._3 >= 70}
pop_songs_2k.count // 17186

// Finding the popular artists who gave the popular songs
val pop_songs_per_arts_2k = pop_songs_2k.map(h => (h._6,1)).reduceByKey(_+_)
pop_songs_per_arts_2k.count // 450
pop_songs_per_arts_2k.sortBy(_._2,false).toDF("ARTISTS","NUMBER OF HITS").show(20) // Popular number of songs per artists in 90s
/*
+--------------------+--------------+
|             ARTISTS|NUMBER OF HITS|
+--------------------+--------------+
|     ['Linkin Park']|           270|
|          ['Eminem']|           268|
|        ['Coldplay']|           239|
|   ['Mother Mother']|           216|
|  ['Britney Spears']|           189|
|         ['Rihanna']|           189|
|['My Chemical Rom...|           162|
|      ['Kanye West']|           162|
|        ['Maroon 5']|           161|
|   ['Avril Lavigne']|           160|
|       ['Green Day']|           146|
|      ['Nickelback']|           141|
|  ['Arctic Monkeys']|           135|
|      ['John Mayer']|           135|
|     ['The Killers']|           135|
| ['Black Eyed Peas']|           134|
|['System Of A Down']|           134|
|['Red Hot Chili P...|           132|
|   ['Michael Bublé']|           131|
|         ['50 Cent']|           131|
+--------------------+--------------+
only showing top 20 rows
*/

// Percentage of Popular songs in 90 decade
val perc_popartists_2k = pop_songs_2k.count/early_twok.count.toDouble *100
println("********************************  % of popular songs in 2k  :  " + perc_popartists_2k )
/*
********************************  % of popular songs in 2k  :  9.123678776006413
*/

// Finding POPULAR AND UNPOPULAR ARTISTS

// Finding POPULAR Artists
val unique_artists_2k = early_twok.map(h => (h._6,1)).reduceByKey(_+_)   // List of unique Artists from 2000's - key value pair
val mostsongs_artists_2k = unique_artists_2k.filter(h => h._2 >= 300)    // Filtering the Artists with the most number of songs
println("******************************** NO. of artists with more than 300 songs: " + mostsongs_artists_2k.count) 
/*
******************************** NO. of artists with more than 300 songs: 76
*/

val freq2_key = mostsongs_artists_2k.map(h => (h._1));                // Collecting just the Artists with the most number of songs in 2k (i.e. Key from KVpair)
val Setsortfreq_2k = freq2_key.collect.toSet                          // Creating a set of Artists with the most number of songs in 2k 
val data_freq2k = early_twok.filter{h => Setsortfreq_2k.contains(h._6)}   // Filtering the data of Artists with the most number of songs in 2k
val pop1_songs_2k = data_freq2k.filter{h => h._3 >= 70} //The number of songs which are popular and belong to an artist with no.of songs>300 

val total_pop_songs_2k = mostsongs_artists_2k.map(_._2).reduce(_+_) // The total number of songs of the artists with no.of songs>300 is = 42765
val Frac_pop_2k = pop1_songs_2k.count/total_pop_songs_2k.toDouble *100
println("********************************  Fraction of popular songs from the popular artists : " + Frac_pop_2k) 
/*
********************************  Fraction of popular songs from the popular artists : 10.700339062317315
*/ 


// Finding UNPOPULAR Artists
val leastsongs_artists_2k = unique_artists_2k.filter(h => h._2 <= 100)   //Filtering the Artists with less number of songs
println("******************************** NO. of artists with less than 100 songs: "+ leastsongs_artists_2k.count)
/*
******************************** NO. of artists with less than 100 songs: 8793
*/ 


//val sortby_popartists_2k = data_pop2k.sortBy(_._6)                 
val freq2_key_least = leastsongs_artists_2k.map(h => (h._1))                // Collecting just the Artists with the most number of songs in 2k (i.e. Key from KVpair)
val Setsortfreq_least_2k = freq2_key_least.collect.toSet                          // Creating a set of Artists with the most number of songs in 2k 
val data_freq_least2k = early_twok.filter{h => Setsortfreq_least_2k.contains(h._6)} 
val pop_least_2k = data_freq_least2k.filter{h => h._3 >= 70} // the number of songs which are popular and belong to an artist with no.of songs<100 
val total_least_songs_2k = leastsongs_artists_2k.map(_._2).reduce(_+_) // The total number of songs of the artists with no.of songs<100 is  111691
val Frac_unpop_2k = pop_least_2k.count/total_least_songs_2k.toDouble *100

println("********************************  Fraction of popular songs from the unpopular artists : " + Frac_unpop_2k )
/*
********************************  Fraction of popular songs from the unpopular artists : 6.4803789025078125
*/  

/*
CONCLUSION - early  2k : The fraction of popular songs belonging to popular artists is high compared to the popular songs from unpopular artists in 2k
*/

println("------------------------------------------------ 2k ------------------------------------------------------------")
//Finding POPULAR SONGS in early 2k

// Finding the songs with popularity greater than 70 
val pop_songs_2k_1 = twok.filter{h => h._3 >= 70}
pop_songs_2k_1.count // 82072


// Finding the popular artists who gave the popular songs
val pop_songs_per_arts_2k_1 = pop_songs_2k_1.map(h => (h._6,1)).reduceByKey(_+_)
pop_songs_per_arts_2k_1.count //2881
pop_songs_per_arts_2k_1.sortBy(_._2,false).toDF("ARTISTS","NUMBER OF HITS").show(20) // Popular number of songs per artists in 90s
/*
+--------------------+--------------+
|             ARTISTS|NUMBER OF HITS|
+--------------------+--------------+
|    ['Taylor Swift']|          1305|
|   ['One Direction']|           996|
|             ['BTS']|           986|
|   ['Billie Eilish']|           729|
|    ['Harry Styles']|           659|
|      ['Juice WRLD']|           648|
|           ['Drake']|           573|
|      ['The Weeknd']|           556|
|      ['Ed Sheeran']|           540|
|    ['XXXTENTACION']|           450|
|   ['Justin Bieber']|           424|
|      ['Bruno Mars']|           420|
|    ['Travis Scott']|           414|
|   ['Ariana Grande']|           407|
|['Twenty One Pilo...|           387|
|       ['Bad Bunny']|           381|
| ['Imagine Dragons']|           381|
|     ['Post Malone']|           380|
|      ['Katy Perry']|           345|
|    ['Shawn Mendes']|           335|
+--------------------+--------------+
only showing top 20 rows
*/

// Percentage of Popular songs in 90 decade
val perc_popartists_2k_1 = pop_songs_2k_1.count/twok.count.toDouble *100
println("********************************  % of popular songs in 2k  :  " + perc_popartists_2k_1 )
/*
********************************  % of popular songs in 2k  :  21.509141227775913
*/

// Finding POPULAR AND UNPOPULAR ARTISTS

// Finding POPULAR Artists
val unique_artists_2k_1 = twok.map(h => (h._6,1)).reduceByKey(_+_)   // List of unique Artists from 2000's - key value pair
val mostsongs_artists_2k_1 = unique_artists_2k_1.filter(h => h._2 >= 300)    // Filtering the Artists with the most number of songs
println("******************************** NO. of artists with more than 300 songs: " + mostsongs_artists_2k_1.count) 
/*
******************************** NO. of artists with more than 300 songs: 164
*/

val freq2_key_1 = mostsongs_artists_2k_1.map(h => (h._1));                // Collecting just the Artists with the most number of songs in 2k (i.e. Key from KVpair)
val Setsortfreq_2k_1 = freq2_key_1.collect.toSet                          // Creating a set of Artists with the most number of songs in 2k 
val data_freq2k_1 = twok.filter{h => Setsortfreq_2k_1.contains(h._6)}   // Filtering the data of Artists with the most number of songs in 2k
val pop_songs_2k_1 = data_freq2k_1.filter{h => h._3 >= 70} //The number of songs which are popular and belong to an artist with no.of songs>300 

val total_pop_songs_2k_1 = mostsongs_artists_2k_1.map(_._2).reduce(_+_) // The total number of songs of the artists with no.of songs>300 is = 113080
val Frac_pop_2k_1 = pop_songs_2k_1.count/total_pop_songs_2k_1.toDouble *100
println("********************************  Fraction of popular songs from the popular artists : " + Frac_pop_2k_1) 
/*
********************************  Fraction of popular songs from the popular artists : 15.887866996816413
*/ 


// Finding UNPOPULAR Artists
val leastsongs_artists_2k_1 = unique_artists_2k_1.filter(h => h._2 <= 100)   //Filtering the Artists with less number of songs
println("******************************** NO. of artists with less than 100 songs: "+ leastsongs_artists_2k_1.count)
/*
******************************** NO. of artists with less than 100 songs: 17462
*/ 


//val sortby_popartists_2k = data_pop2k.sortBy(_._6)                 
val freq2_key_least_1 = leastsongs_artists_2k_1.map(h => (h._1))                // Collecting just the Artists with the most number of songs in 2k (i.e. Key from KVpair)
val Setsortfreq_least_2k_1 = freq2_key_least_1.collect.toSet                          // Creating a set of Artists with the most number of songs in 2k 
val data_freq_least2k_1 = twok.filter{h => Setsortfreq_least_2k_1.contains(h._6)} 
val pop_least_2k_1 = data_freq_least2k_1.filter{h => h._3 >= 70} // the number of songs which are popular and belong to an artist with no.of songs<100 
val total_least_songs_2k_1 = leastsongs_artists_2k_1.map(_._2).reduce(_+_) // The total number of songs of the artists with no.of songs<100 is  57498
val Frac_unpop_2k_1 = pop_least_2k_1.count/total_least_songs_2k_1.toDouble *100

println("********************************  Fraction of popular songs from the unpopular artists : " + Frac_unpop_2k_1 )
/*
********************************  Fraction of popular songs from the unpopular artists : 24.380623748319337
*/  

/*
CONCLUSION - 2k : The fraction of popular songs belonging to popular artists is very high compared to the popular songs from unpopular artists in 2k
*/



println("***************************************** HOW VALENCE OF SONGS RELATED TO POPULARITY  *************************************")

// Analysing the valence and its corresponding popularity 
//Tracks with high valence sound more positive (e.g. happy, cheerful, euphoric), while tracks with low valence sound more negative (e.g. sad, depressed, angry)


println("------------------------------------------------ 80's ------------------------------------------------------------")

val high_val_80s = eighties.filter(h => h._18 > 0.8).map(h => (h._3,h._18)) // Filtering the high valence songs and Creating KVpair of (Popularity, Valence)
println("********************************  Songs with high valence in 80  : " + high_val_80s.count )
/*
********************************  Songs with high valence in 90  : 70233
*/
val high_val_pop_80s = high_val_80s.filter(_._1 > 70)
println("********************************  Popular high valence songs in 80  : "+ high_val_pop_80s.count )
/*
********************************  Popular high valence songs in 80  : 1817
*/
high_val_pop_80s.toDF("Popularity","High Valence").show
/*
+----------+------------+
|Popularity|High Valence|
+----------+------------+
|        79|       0.902|
|        78|       0.813|
|        72|       0.944|
|        72|       0.961|
|        71|       0.871|
|        78|       0.971|
|        76|       0.958|
|        75|       0.901|
|        75|         0.9|
|        74|       0.962|
|        74|       0.888|
|        72|       0.919|
|        72|       0.845|
|        85|       0.847|
|        80|       0.915|
|        80|        0.82|
|        76|       0.823|
|        73|       0.866|
|        73|       0.971|
|        72|       0.961|
+----------+------------+
only showing top 20 rows
*/
val Frac_high_pop_80 = high_val_pop_80s.count/high_val_80s.count.toDouble *100 
println("********************************  Fraction of popular high valence songs  : " + Frac_high_pop_80)
/*
********************************  Fraction of popular high valence songs  : 2.5871029288226333
*/

val low_val_80s = eighties.filter(h => h._18 < 0.2).map(h => (h._3,h._18)) // Filtering the high valence songs and Creating KVpair of (Popularity, Valence)
println("********************************  Songs with low valence in 80  : " + low_val_80s.count)
/*
********************************  Songs with low valence in 80  : 24802
*/
val low_val_pop_80s = low_val_80s.filter(_._1 > 70)
println("******************************** Popular Songs with low valence in 80 : " + low_val_pop_80s.count )
/*
******************************** Popular Songs with low valence in 80 : 267
*/
low_val_pop_80s.toDF("Popularity","Low Valence").show
/*
+----------+-----------+
|Popularity|Low Valence|
+----------+-----------+
|        72|      0.174|
|        76|      0.189|
|        75|       0.19|
|        73|      0.144|
|        78|      0.188|
|        72|      0.139|
|        81|      0.113|
|        75|      0.168|
|        79|      0.194|
|        77|      0.126|
|        72|      0.174|
|        75|       0.19|
|        76|      0.189|
|        78|      0.188|
|        72|      0.139|
|        81|      0.113|
|        75|      0.168|
|        79|      0.194|
|        77|      0.126|
|        72|      0.174|
+----------+-----------+
only showing top 20 rows
*/
val Frac_low_pop_80 = low_val_pop_80s.count/low_val_80s.count.toDouble *100 
println("********************************  Fraction of popular low valence songs  : " + Frac_low_pop_80)
/*
********************************  Fraction of popular low valence songs  : 1.0765260866059188
*/

/*

CONCLUSION: Audience liked songs with high valence better than low valence in 80s
*/


println("------------------------------------------------ 90's ------------------------------------------------------------")

val high_val_90s = nineties.filter(h => h._18 > 0.8).map(h => (h._3,h._18)) // Filtering the high valence songs and Creating KVpair of (Popularity, Valence)
println("********************************  Songs with high valence in 90  : " + high_val_90s.count)
/*
********************************  Songs with high valence in 90  : 59850
*/
val high_val_pop_90s = high_val_90s.filter(_._1 > 70)
println("********************************  Popular high valence songs in 90  : " + high_val_pop_90s.count)
/*
********************************  Popular high valence songs in 2k  : 1639
*/

high_val_pop_90s.toDF("Popularity","High Valence").show
/*
+----------+------------+
|Popularity|High Valence|
+----------+------------+
|        74|       0.866|
|        71|       0.961|
|        83|       0.803|
|        73|       0.805|
|        73|       0.949|
|        72|       0.844|
|        72|       0.836|
|        71|       0.891|
|        71|       0.824|
|        71|        0.93|
|        71|       0.817|
|        74|       0.962|
|        72|       0.819|
|        71|       0.827|
|        74|       0.858|
|        71|       0.899|
|        76|        0.97|
|        76|       0.814|
|        75|       0.818|
|        71|       0.858|
+----------+------------+
only showing top 20 rows
*/
val Frac_high_pop_90 = high_val_pop_90s.count/high_val_90s.count.toDouble *100  
println("********************************  Fraction of popular high valence songs  : " + Frac_high_pop_90)
/*
********************************  Fraction of popular high valence songs  : 2.7385129490392646
*/

val low_val_90s = nineties.filter(h => h._18 < 0.2).map(h => (h._3,h._18)) // Filtering the high valence songs and Creating KVpair of (Popularity, Valence)
println("********************************  Songs with low valence in 90  : " + low_val_90s.count)
/*
********************************  Songs with low valence in 90  : 27923
*/
val low_val_pop_90s = low_val_90s.filter(_._1 > 70)
println("******************************** Popular Songs with low valence in 90 : " + low_val_pop_90s.count)
/*
********************************  Songs with low valence in 90 : 594
*/
low_val_pop_90s.toDF("Popularity","Low Valence").show
/*
+----------+-----------+
|Popularity|Low Valence|
+----------+-----------+
|        74|      0.161|
|        71|      0.166|
|        80|      0.158|
|        78|       0.11|
|        76|      0.166|
|        74|      0.161|
|        83|      0.104|
|        75|      0.175|
|        72|      0.178|
|        71|      0.147|
|        77|      0.147|
|        73|     0.0831|
|        72|      0.139|
|        71|      0.159|
|        72|      0.135|
|        76|      0.177|
|        76|     0.0382|
|        74|      0.118|
|        71|      0.181|
|        77|      0.138|
+----------+-----------+
only showing top 20 rows
*/

val Frac_low_pop_90 = low_val_pop_90s.count/low_val_90s.count.toDouble *100 
println("********************************  Fraction of popular low valence songs  : " + Frac_low_pop_90)
/*
********************************  Fraction of popular low valence songs  : 2.127278587544318
*/

/*
CONCLUSION: Audience like sad songs as much as they do happy, cheerful songs in 90
*/


println("------------------------------------------------ early 2k ------------------------------------------------------------")

val high_val_2k = early_twok.filter(h => h._18 > 0.8).map(h => (h._3,h._18)) // Filtering the high valence songs and Creating KVpair of (Popularity, Valence)
println("********************************  Songs with high valence in 2k  : " + high_val_2k.count )
/*
********************************  Songs with high valence in 2k  : 38842
*/
val high_val_pop_2k = high_val_2k.filter(_._1 > 70)
println("********************************  Popular high valence songs in 2k  : " + high_val_pop_2k.count)
/*
********************************  Popular high valence songs in 2k  : 2379
*/
high_val_pop_2k.toDF("Popularity","High Valence").show
/*
+----------+------------+
|Popularity|High Valence|
+----------+------------+
|        81|       0.894|
|        77|       0.941|
|        77|       0.867|
|        75|       0.843|
|        74|       0.807|
|        73|       0.866|
|        73|       0.885|
|        73|       0.916|
|        72|       0.837|
|        71|       0.888|
|        80|       0.903|
|        76|        0.87|
|        76|       0.872|
|        76|       0.919|
|        75|       0.869|
|        75|       0.964|
|        75|       0.882|
|        74|       0.839|
|        74|       0.897|
|        73|       0.908|
+----------+------------+
only showing top 20 rows

*/

val Frac_high_pop_2k = high_val_pop_2k.count/high_val_2k.count.toDouble *100 
println("********************************  Fraction of popular high valence songs  : " + Frac_high_pop_2k)
/*
********************************  Fraction of popular high valence songs  : 6.124813346377633
*/

val low_val_2k = early_twok.filter(h => h._18 < 0.2).map(h => (h._3,h._18)) // Filtering the high valence songs and Creating KVpair of (Popularity, Valence)
println("********************************  Songs with low valence in 2k : " + low_val_2k.count )
/*
********************************  Songs with low valence in 2k : 20097
*/
val low_val_pop_2k = low_val_2k.filter(_._1 > 70)
println("********************************  Popular low  valence songs in 2k  : " +low_val_pop_2k.count)
/*
********************************  Popular low  valence songs in 2k  : 1258
*/

low_val_pop_2k.toDF("Popularity","Low Valence").show
/*
+----------+-----------+
|Popularity|Low Valence|
+----------+-----------+
|        75|      0.165|
|        73|      0.195|
|        71|      0.125|
|        71|      0.116|
|        71|      0.195|
|        71|     0.0559|
|        85|        0.1|
|        76|      0.163|
|        75|      0.198|
|        73|     0.0992|
|        72|      0.147|
|        72|      0.185|
|        72|      0.136|
|        72|      0.193|
|        81|     0.0681|
|        80|      0.166|
|        75|      0.101|
|        73|     0.0902|
|        78|      0.194|
|        78|      0.146|
+----------+-----------+
only showing top 20 rows
*/
val Frac_low_pop_2k = low_val_pop_2k.count/low_val_2k.count.toDouble *100 
println("********************************  Fraction of popular low valence songs  : " + Frac_low_pop_2k)
/*
********************************  Fraction of popular low valence songs  : 6.2596407423993625
*/
/*
CONCLUSION: Audience like sad songs as much as they do happy, cheerful songs in 2k
*/

println("------------------------------------------------ 2k ------------------------------------------------------------")

val high_val_2k_1 = twok.filter(h => h._18 > 0.8).map(h => (h._3,h._18)) // Filtering the high valence songs and Creating KVpair of (Popularity, Valence)
println("********************************  Songs with high valence in 2k  : " + high_val_2k_1.count )
/*
********************************  Songs with high valence in 2k  : 48807
*/
val high_val_pop_2k_1 = high_val_2k_1.filter(_._1 > 70)
println("********************************  Popular high valence songs in 2k  : " + high_val_pop_2k_1.count)
/*
********************************  Popular high valence songs in 2k  : 8491
*/
high_val_pop_2k_1.toDF("Popularity","High Valence").show
/*
+----------+------------+
|Popularity|High Valence|
+----------+------------+
|        80|       0.816|
|        79|       0.828|
|        78|       0.801|
|        76|       0.833|
|        76|       0.852|
|        76|       0.835|
|        74|       0.957|
|        73|       0.828|
|        73|       0.929|
|        73|        0.94|
|        72|       0.844|
|        72|       0.879|
|        72|       0.972|
|        72|       0.938|
|        71|       0.876|
|        85|       0.965|
|        79|       0.922|
|        77|       0.877|
|        76|       0.814|
|        75|       0.927|
+----------+------------+
only showing top 20 rows
*/

val Frac_high_pop_2k_1 = high_val_pop_2k_1.count/high_val_2k_1.count.toDouble *100 
println("********************************  Fraction of popular high valence songs  : " + Frac_high_pop_2k_1)
/*
********************************  Fraction of popular high valence songs  : 17.397094679041942
*/

val low_val_2k_1 = twok.filter(h => h._18 < 0.2).map(h => (h._3,h._18)) // Filtering the high valence songs and Creating KVpair of (Popularity, Valence)
println("********************************  Songs with low valence in 2k : " + low_val_2k_1.count )
/*
********************************  Songs with low valence in 2k : 65068
*/
val low_val_pop_2k_1 = low_val_2k_1.filter(_._1 > 70)
println("********************************  Popular low  valence songs in 2k  : " +low_val_pop_2k_1.count)
/*
********************************  Popular low  valence songs in 2k  : 9217
*/

low_val_pop_2k_1.toDF("Popularity","Low Valence").show
/*
+----------+-----------+
|Popularity|Low Valence|
+----------+-----------+
|        77|      0.111|
|        75|      0.103|
|        74|      0.127|
|        73|     0.0401|
|        72|     0.0963|
|        71|      0.169|
|        74|     0.0783|
|        74|       0.11|
|        73|      0.182|
|        71|     0.0822|
|        71|       0.19|
|        71|      0.127|
|        78|      0.144|
|        77|      0.125|
|        76|      0.176|
|        75|      0.189|
|        75|     0.0789|
|        75|      0.189|
|        73|      0.137|
|        72|      0.186|
+----------+-----------+
only showing top 20 rows
*/
val Frac_low_pop_2k_1 = low_val_pop_2k_1.count/low_val_2k_1.count.toDouble *100 
println("********************************  Fraction of popular low valence songs  : " + Frac_low_pop_2k_1)
/*
********************************  Fraction of popular low valence songs  : 14.165181041372103
*/
/*
CONCLUSION: Audience happy cheerful songs better than sad songs in 2k
*/


println("***************************************** INSTRUMENTAL SONGS VS VOCAL SONGS   ***************************************")

println("------------------------------------------------ 80's ------------------------------------------------------------")
//Analysing Instrumentalness in 80s
println("*******************************Instrumentalness in 80s  ******************************")

val instr_high_80s = eighties.filter(h =>h._16 > 0.8).map(h => (h._3,h._16))// Filtering the high instrumental songs and Creating KVpair of (Popularity, Instrumentalness)
instr_high_80s.count   //  12982 - No. of Instrumental songs in 80
val instr_high_pop_80s = instr_high_80s.filter(_._1 > 70)
instr_high_pop_80s.count  // 53 - No of popular instrumental songs
val Frac_instr_pop_80s = instr_high_pop_80s.count/instr_high_80s.count.toDouble *100 // Fraction of instrumental songs that are popular in 80
println("********************************  Fraction of popular intrumental songs in 80s : " + Frac_instr_pop_80s)
/*
********************************  Fraction of popular intrumental songs :  0.40825758742874746
*/

val voc_high_80s = eighties.filter(h =>h._16 < 0.7).map(h => (h._3,h._16))// Filtering the vocal songs and Creating KVpair of (Popularity, Instrumentalness)
voc_high_80s.count //254896 - No. of vocal songs in 80
val voc_high_pop_80s = voc_high_80s.filter(_._1 > 70)
voc_high_pop_80s.count // 5872 - No of popular vocal songs
val Frac_voc_pop_80s = voc_high_pop_80s.count/voc_high_80s.count.toDouble *100  // Fraction of vocal songs that are popular in 80
println("********************************  Fraction of popular vocal songs in 80s : " + Frac_voc_pop_80s)
/*
********************************  Fraction of popular vocal songs  :  2.3036846400100432
*/

/*
CONCLUSION: Audience clearly preferred vocal songs more than instrumental songs in 80s
*/

println("------------------------------------------------ 90's ------------------------------------------------------------")
//Analysing Instrumentalness in 90s
println("*******************************Instrumentalness in 90s  ******************************")

val instr_high_90 = nineties.filter(h =>h._16 > 0.8).map(h => (h._3,h._16))// Filtering the high instrumental songs and Creating KVpair of (Popularity, Instrumentalness)
instr_high_90.count // 12854 - no of intrumental songs in 90s
val instr_high_pop_90 = instr_high_90.filter(_._1 > 70)
instr_high_pop_90.count // 54 - no of popular Instrumental songs in 90s
val Frac_instr_pop_90 = instr_high_pop_90.count/instr_high_90.count.toDouble *100
println("********************************  Fraction of popular intrumental songs in 90s  : " + Frac_instr_pop_90)
/*
********************************  Fraction of popular intrumental songs in 90s  : 0.4201026917690991
*/

val voc_90 = nineties.filter(h =>h._16 < 0.7).map(h => (h._3,h._16))// Filtering the vocal songs and Creating KVpair of (Popularity, Instrumentalness)
voc_90.count //249757 - no of vocal songs in 90
val voc_high_pop_90 = voc_90.filter(_._1 > 70)
voc_high_pop_90.count // 7754 - no of popular vocal songs in 90
val Frac_voc_pop_90 = voc_high_pop_90.count/voc_90.count.toDouble *100  
println("********************************  Fraction of popular vocal songs in 90s : " + Frac_voc_pop_90)
/*
********************************  Fraction of popular vocal songs in 90s : 3.104617688393118
*/

/*
CONCLUSION: Audience clearly preferred vocal songs ten times more than instrumental songs in 90s
*/

println("------------------------------------------------ early 2k ------------------------------------------------------------")
//Analysing Instrumentalness in early 2k 
println("*******************************Instrumentalness in 2ks  ******************************")

val instr_high_2k = early_twok.filter(h =>h._16 > 0.8).map(h => (h._3,h._16))// Filtering the high instrumental songs and Creating KVpair of (Popularity, Instrumentalness)
instr_high_2k.count // 14202 - no of instrumental songs in 2k
val instr_high_pop_2k = instr_high_2k.filter(_._1 > 70)
instr_high_pop_2k.count // 145 - no of popular Instrumental songs in 2k
val Frac_instr_pop_2k = instr_high_pop_2k.count/instr_high_2k.count.toDouble *100 
println("********************************  Fraction of popular intrumental songs in 2k  : " + Frac_instr_pop_2k)
/*
********************************  Fraction of popular intrumental songs in 2k  : 1.02098296014645847
*/

val voc_2k = early_twok.filter(h =>h._16 < 0.7).map(h => (h._3,h._16))// Filtering the vocal songs and Creating KVpair of (Popularity, Instrumentalness)
voc_2k.count  //484558 - no of vocal songs in 2k 
val voc_high_pop_2k = voc_2k.filter(_._1 > 70)
voc_high_pop_2k.count // 88485 - no popular vocal songs in 2k
val Frac_voc_pop_2k = voc_high_pop_2k.count/voc_2k.count.toDouble *100  //0.24
println("********************************  Fraction of popular vocal songs in 2k : " + Frac_voc_pop_2k)
/*
********************************  Fraction of popular vocal songs in 2k : 8.214237463360766
*/

/*
CONCLUSION: It is very evident that audience of early 2k like vocal song more than instrumental songs 
*/

println("------------------------------------------------ 2k ------------------------------------------------------------")
//Analysing Instrumentalness in 2k 
println("*******************************Instrumentalness in 2ks  ******************************")

val instr_high_2k_1 = twok.filter(h =>h._16 > 0.8).map(h => (h._3,h._16))// Filtering the high instrumental songs and Creating KVpair of (Popularity, Instrumentalness)
instr_high_2k_1.count // 56136 - no of instrumental songs in 2k
val instr_high_pop_2k_1 = instr_high_2k_1.filter(_._1 > 70)
instr_high_pop_2k_1.count // 1049 - no of popular Instrumental songs in 2k
val Frac_instr_pop_2k_1 = instr_high_pop_2k_1.count/instr_high_2k_1.count.toDouble *100 
println("********************************  Fraction of popular intrumental songs in 2k  : " + Frac_instr_pop_2k_1)
/*
********************************  Fraction of popular intrumental songs in 2k  : 1.8686760723956106
*/

val voc_2k_1 = twok.filter(h =>h._16 < 0.7).map(h => (h._3,h._16))// Filtering the vocal songs and Creating KVpair of (Popularity, Instrumentalness)
voc_2k_1.count  //314317 - no of vocal songs in 2k 
val voc_high_pop_2k_1 = voc_2k_1.filter(_._1 > 70)
voc_high_pop_2k_1.count //  74501 - no popular vocal songs in 2k
val Frac_voc_pop_2k_1 = voc_high_pop_2k_1.count/voc_2k_1.count.toDouble *100  //0.24
println("********************************  Fraction of popular vocal songs in 2k : " + Frac_voc_pop_2k_1)
/*
********************************  Fraction of popular vocal songs in 2k : 23.70250415981318
*/

/*
CONCLUSION: It is clearly evident that audience of 2k like vocal song way more than instrumental songs 
*/

println("***************************************** SONGS RECORDED LIVE VS SONGS RECORDED IN STUDIO  ***************************************")

println("------------------------------------------------ 80's ------------------------------------------------------------")
//Analysing Liveness
println("*******************************Liveness in 80s  ******************************")
val live_high_80s =eighties.filter(h =>h._17 > 0.8).map(h => (h._3,h._17))// Filtering the live performed songs and Creating KVpair of (Popularity, Liveness)
live_high_80s.count //  5174 - No of songs that were recorded when performed live
val live_high_pop_80s = live_high_80s.filter(_._1 > 70)
live_high_pop_80s.count //0
val Frac_live_pop_80s = live_high_pop_80s.count/live_high_80s.count.toDouble *100 
println("********************************  Fraction of popular live recorded songs in 80s : " + Frac_live_pop_80s)
/*
********************************  Fraction of popular live recorded songs in 80s : 0
*/

val studio_80s = eighties.filter(h =>h._17 < 0.7).map(h => (h._3,h._16))// Filtering the recorded songs and Creating KVpair of (Popularity, studio recorded)
studio_80s.count // 254896 - no of songs that were recorded in studio in 80s
val studio_pop_80s = studio_80s.filter(_._1 > 70)
studio_pop_80s.count //  5872 - No of popular studio recorded songs in 80s
val Frac_studio_pop_80s = studio_pop_80s.count/studio_80s.count.toDouble *100  //2.3
println("********************************  Fraction of popular studio recorded songs in 80s : " + Frac_studio_pop_80s)
/*
********************************  Fraction of popular studio recorded songs in 80s : 2.3036846400100432
*/

/*
CONCLUSION: People did not listen to any live music in the 1980s and only listened to the studio recorded music.
*/


println("------------------------------------------------ 90's ------------------------------------------------------------")

println("*******************************Liveness in 90s  ******************************")
val live_high_90s = nineties.filter(h =>h._17 > 0.8).map(h => (h._3,h._17))// Filtering the live performed songs and Creating KVpair of (Popularity, Liveness)
live_high_90s.count // 6112 - No of songs that were recorded when performed live in 90s
val live_high_pop_90s = live_high_90s.filter(_._1 > 70)
live_high_pop_90s.count // 41 - No of popular live recorded songs in 90s
val Frac_live_pop_90s = live_high_pop_90s.count/live_high_90s.count.toDouble *100  
println("********************************  Fraction of popular live recorded songs in 90s : " + Frac_live_pop_90s)
/*
********************************  Fraction of popular live recorded songs in 90s : 0.6708115183246074
*/

val studio_90s = nineties.filter(h =>h._17 < 0.7).map(h => (h._3,h._16))// Filtering the recorded songs and Creating KVpair of (Popularity, studio recorded)
studio_90s.count  // 249757 - no of songs that were recorded in studio in 90s
val studio_pop_90s = studio_90s.filter(_._1 > 70)
studio_pop_90s.count // 7754 - no of popular studio recorded songs in 90s
val Frac_studio_pop_90s = studio_pop_90s.count/studio_90s.count.toDouble *100  
println("********************************  Fraction of popular studio recorded songs in 90s : " + Frac_studio_pop_90s)
/*
********************************  Fraction of popular studio recorded songs in 90s : 3.104617688393118
*/
/*
CONCLUSION: People in the 90s liked studio recorded songs more than live ones
*/
println("------------------------------------------------  early 2k------------------------------------------------------------")

println("*******************************Liveness in 2ks  ******************************")
val live_high_2k = early_twok.filter(h =>h._17 > 0.8).map(h => (h._3,h._17))// Filtering the live performed songs and Creating KVpair of (Popularity, Liveness)
live_high_2k.count // 5978 - No of live recorded songs in 2k
val live_high_pop_2k = live_high_2k.filter(_._1 > 70)
live_high_pop_2k.count // 103 - No of popular live recorded songs in 2k
val Frac_live_pop_2k = live_high_pop_2k.count/live_high_2k.count.toDouble *100 
println("********************************  Fraction of popular live recorded songs in 2k : " + Frac_live_pop_2k)
/*
********************************  Fraction of popular live recorded songs in 2k : 1.722984275677484
*/
val studio_2k = early_twok.filter(h =>h._17 < 0.7).map(h => (h._3,h._16))// Filtering the recorded songs and Creating KVpair of (Popularity, studio recorded)
studio_2k.count  // 179024 - No of studio recorded songs 
val studio_pop_2k = studio_2k.filter(_._1 > 70)
studio_pop_2k.count //14016 - No of popular studio recorded songs in 2k
val Frac_studio_pop_2k = studio_pop_2k.count/studio_2k.count.toDouble *100  
println("********************************  Fraction of popular studio recorded songs in 2k: " + Frac_studio_pop_2k)
/*
********************************  Fraction of popular studio recorded songs in 2k: 7.829117883635714
*/
/*
CONCLUSION: Spotify listeners like to listen to studio recorded songs more than live performances which are recorded in early 2k 
*/

println("------------------------------------------------ 2k------------------------------------------------------------")

println("*******************************Liveness in 2ks  ******************************")
val live_high_2k_1 = twok.filter(h =>h._17 > 0.8).map(h => (h._3,h._17))// Filtering the live performed songs and Creating KVpair of (Popularity, Liveness)
live_high_2k_1.count // 13220 - No of live recorded songs in 2k
val live_high_pop_2k_1 = live_high_2k_1.filter(_._1 > 70)
live_high_pop_2k_1.count // 179 - No of popular live recorded songs in 2k
val Frac_live_pop_2k_1 = live_high_pop_2k_1.count/live_high_2k_1.count.toDouble *100 
println("********************************  Fraction of popular live recorded songs in 2k : " + Frac_live_pop_2k_1)
/*
********************************  Fraction of popular live recorded songs in 2k : 1.3540090771558246
*/
val studio_2k_1 = twok.filter(h =>h._17 < 0.7).map(h => (h._3,h._16))// Filtering the recorded songs and Creating KVpair of (Popularity, studio recorded)
studio_2k_1.count  // 359650 - No of studio recorded songs 
val studio_pop_2k_1 = studio_2k_1.filter(_._1 > 70)
studio_pop_2k_1.count //75072 - No of popular studio recorded songs in 2k
val Frac_studio_pop_2k_1 = studio_pop_2k_1.count/studio_2k_1.count.toDouble *100  
println("********************************  Fraction of popular studio recorded songs in 2k: " + Frac_studio_pop_2k_1)
/*
********************************  Fraction of popular studio recorded songs in 2k: 20.873627137494786
*/
/*
CONCLUSION: Spotify listeners like to listen to studio recorded songs way more than live performances which are recorded in 2k 
*/


println("***************************************** HOW IS DANCEABILITY AND VALENCE RELATED?  ***************************************")
//Relation between Danceability and Valence

println("------------------------------------------------ 80's ------------------------------------------------------------")
println("*******************************Relation between Danceability and Valence in 80s ******************************")
val high_dance_eng_80s = eighties.filter(h => h._9 > 0.8).map(h=> (h._9,h._10)) // filtering high danceable songs and creating KVpair - (danceablitly,energy)
val perc_dance_songs_80s = high_dance_eng_80s.count/eighties.count .toDouble*100 
println("******************************** Percentage of danceable songs in 80s : "+ perc_dance_songs_80s)
/*
******************************** Percentage of danceable songs in 80s : 6.611940902056077
*/
val high_eng_in_dance_80s = high_dance_eng_80s.filter(_._2 > 0.6)
val perc_high_eng_in_dance_80s = high_eng_in_dance_80s.count/high_dance_eng_80s.count.toDouble *100 //48.7
println("******************************** Percentage of danceable songs which also has high energy in 80s : "+ perc_high_eng_in_dance_80s)
/*
******************************** Percentage of danceable songs which also has high energy in 80s : 48.87690425886801
*/
/*
CONCLUSION: Less than half the number of danceable songs have high energy in the 1980s.
Comparing the results from 2000's, we can also conclude that as the years passed by ,danceable songs have more energy in them.
*/

println("------------------------------------------------ 90's ------------------------------------------------------------")

println("*******************************Relation between Danceability and Valence in 90s ******************************")
val high_dance_eng_90s = nineties.filter(h => h._9 > 0.8).map(h=> (h._9,h._10)) // filtering high danceable songs and creating KVpair - (danceablitly,energy)
val perc_dance_songs_90s = high_dance_eng_90s.count/nineties.count .toDouble*100
println("******************************** Percentage of danceable songs in 90s : "+ perc_dance_songs_90s)
/*
******************************** Percentage of danceable songs in 90s : 8.873097817930786
*/
val high_eng_in_dance_90s = high_dance_eng_90s.filter(_._2 > 0.6)
val perc_high_eng_in_dance_90s = high_eng_in_dance_90s.count/high_dance_eng_90s.count.toDouble *100 
println("******************************** Percentage of danceable songs which also has high energy in 90s : "+ perc_high_eng_in_dance_90s)
/*
******************************** Percentage of danceable songs which also has high energy in 80s : 60.739957716701895
*/
/*
CONCLUSION: We can clearly see that danceable songs have high energy and there is a increase in the number of danceable songs in 90s
*/

println("------------------------------------------------ early 2k ------------------------------------------------------------")

println("*******************************Relation between Danceability and Valence in early 2ks ******************************")
val high_dance_eng_2k = early_twok.filter(h => h._9 > 0.8).map(h=> (h._9,h._10)) // filtering high danceable songs and creating KVpair - (danceablitly,energy)
val perc_dance_songs_2k = high_dance_eng_2k.count/early_twok.count .toDouble*100
println("******************************** Percentage of danceable songs in 2k : "+ perc_dance_songs_2k)
/*
******************************** Percentage of danceable songs in 2k : 8.83381908720742
*/
val high_eng_in_dance_2k = high_dance_eng_2k.filter(_._2 > 0.6)
val perc_high_eng_in_dance_2k = high_eng_in_dance_2k.count/high_dance_eng_2k.count.toDouble *100 
println("******************************** Percentage of danceable songs which also has high energy in early2k : "+ perc_high_eng_in_dance_2k)
/*
******************************** Percentage of danceable songs which also has high energy in early2k : 71.50841346153847
*/
/*
CONCLUSION: Significant number of danceable songs have high energy
*/

println("------------------------------------------------ 2k ------------------------------------------------------------")

println("*******************************Relation between Danceability and Valence in 2ks ******************************")
val high_dance_eng_2k_1 = twok.filter(h => h._9 > 0.8).map(h=> (h._9,h._10)) // filtering high danceable songs and creating KVpair - (danceablitly,energy)
val perc_dance_songs_2k_1 = high_dance_eng_2k_1.count/twok.count.toDouble*100
println("******************************** Percentage of danceable songs in 2k : "+ perc_dance_songs_2k_1)
/*
******************************** Percentage of danceable songs in 2k : 9.13834493458571
*/
val high_eng_in_dance_2k_1 = high_dance_eng_2k_1.filter(_._2 > 0.6)
val perc_high_eng_in_dance_2k_1 = high_eng_in_dance_2k_1.count/high_dance_eng_2k_1.count.toDouble *100 //62.87
println("******************************** Percentage of danceable songs which also has high energy in 2k : "+ perc_high_eng_in_dance_2k_1)
/*
******************************** Percentage of danceable songs which also has high energy in 2k : 58.94920989991109
*/
/*
CONCLUSION: Significant number of danceable songs have high energy and about nine percent of songs in 2k are danceable
*/

println("***************************************** COUNT OF SPEECHINESS IN DIFFERENT DECADES  ***************************************")

// Speechiness - the count of songs with high speechiness in different decades
println("*******************************Speechiness in Different Decades  ******************************")
val twenty_forty = yearly.filter(h => (h._8 >= "1920" && h._8 < "1940")).filter(h => (h._14 >0.8)).count // 73711
val forty_sixty = yearly.filter(h => (h._8 >= "1940" && h._8 < "1960")).filter(h => (h._14 >0.8)).count  // 11369
val sixty_eighty = yearly.filter(h => (h._8 >= "1960" && h._8 < "1980")).filter(h => (h._14 >0.8)).count //4745
val eighty_2k = yearly.filter(h => (h._8 >= "1980" && h._8 < "2000")).filter(h => (h._14 >0.8)).count // 10142
val after_2k = yearly.filter(h => (h._8 >= "2000" && h._8 < "2021")).filter(h => (h._14 >0.8)).count // 1788

/*
CONCLUSION: The number of songs with high speechiness is significantly high during the first two decades and it gradually decreases 
*/


println("****************************************** CORRELATION BETWEEN THE DIFFERENT NUMERIC FEATURES *****************************")

import scala.math.sqrt
def corr_r(n:Long,sx:Float,sy:Float,sxy:Float,sx2:Float,sy2:Float):Double ={
    var r = (((n*sxy) - (sx)*(sy))/(sqrt((n*sx2 - sx2)*(n*sy2 - sy2))))
    r 
}

println("*****************************************  CORRELATION BETWEEN ENERGY AND LOUDNESS **************************************")
val n = yearly.count
val sum_eng = yearly.map(_._10).reduce(_+_)
val sum_loud = yearly.map(_._12).reduce(_+_)
val sum_prod_enlo = yearly.map(h=> (h._10.*(h._12))).reduce(_+_)
val sum_sqr_en = yearly.map(h => (h._10.*(h._10))).reduce(_+_)
val sum_sqr_lo = yearly.map(h => (h._12.*(h._12))).reduce(_+_)

// Correlation between energy and loudness
println("******************************** Correlation between Energy and Loudness : " + corr_r(n,sum_eng,sum_loud,sum_prod_enlo,sum_sqr_en,sum_sqr_lo))
/*
******************************** Correlation between Energy and Loudness : 0.16407601905520444
*/

/*
CONCLUSION - Energy and Loudness share a positive correlation
*/


println("*****************************************  CORRELATION BETWEEN ENERGY AND TEMPO **************************************")
val sum_temp = yearly.map(_._19).reduce(_+_)
val sum_prod_entm = yearly.map(h=> (h._10.*(h._19))).reduce(_+_)
val sum_sqr_tm = yearly.map(h => (h._19.*(h._19))).reduce(_+_)

// Correlation between energy and temp
println("******************************** Correlation between Energy and Tempo : " + corr_r(n,sum_eng,sum_temp,sum_prod_entm,sum_sqr_en,sum_sqr_tm))
/*
******************************** Correlation between Energy and Tempo : 0.032663235528860086
*/

/*
CONCLUSION: Energy and tempo share a very little correlation
*/

println("*****************************************  CORRELATION BETWEEN ACOUSTICNESS AND LOUDNESS **************************************")
val sum_aco = yearly.map(_._15).reduce(_+_)
val sum_prod_aclo = yearly.map(h=> (h._15.*(h._12))).reduce(_+_)
val sum_sqr_ac = yearly.map(h => (h._15.*(h._15))).reduce(_+_)

println("******************************** Correlation between Acousticness and Loudness : " + corr_r(n,sum_aco,sum_loud,sum_prod_aclo,sum_sqr_ac,sum_sqr_lo))
/*
******************************** Correlation between Acousticness and Loudness : -0.13388772774620541
*/

/*
CONCLUSION: Acousticness and Loudness share a negative correlation 
*/

/*
For plotting purpose a RDD with numeric data has been created and dumped as csv file
*/

val Numeric_Rdd =  yearly.map(h => (h._3,h._5,h._9,h._10,h._11,h._12,h._14,h._15,h._16,h._17,h._18,h._19))
Numeric_Rdd.toDF("popularity","explicit","danceability","energy","key","loudness","speech","acoustics","instrument","liveness","valence","tempo").coalesce(1).write.option("header","true").csv("results")
