-- Drop table

DROP TABLE public.simple_data;

CREATE TABLE public.simple_data (
	sirano varchar(50) NULL,
	isim varchar(50) NULL,
	yas int4 NULL,
	meslek varchar(50) NULL,
	sehir varchar(50) NULL,
	aylik_gelir float8 NULL
);


INSERT INTO public.simple_data
(sirano, isim, yas, meslek, sehir, aylik_gelir)
VALUES
(1,'Cemal',35,'Isci','Ankara',3500),
(2,'Ceyda',42,'Memur','Kayseri',4200),
(3,'Timur',30,'Müzisyen','Istanbul',9000),
(4,'Burcu',29,'Pazarlamaci','Ankara',4200),
(5,'Yasemin',23,'','Bursa',4800),
(6,'Ali',33,'Memur','Ankara',4250),
(7,'Dilek',29,'Pazarlamaci','Istanbul',7300),
(8,'Murat',31,'Müzisyen','Istanbul',12000),
(9,'Ahmet',33,'Doktor','Ankara',18000),
(10,'Muhittin',46,'Berber','Istanbul',12000),
(11,'Hicaziye',47,'Tuhafiyeci','Ankara',4800),
(12,'Harun',43,'Tornacý','Ankara',4200),
(13,'Hakký',33,'Memur','Çorum',3750),
(14,'Gülizar',37,'Doktor','Ýzmir',14250),
(15,'Þehmuz',41,'','Ankara',8700),
(16,'Gençay',46,'Berber','Ankara',8800),
(16,'Gençay',46,'Berber','Ankara',8800)
;

select sirano, isim, yas, meslek, sehir, aylik_gelir 
from public.simple_data
limit 5



select * from simple_data
where sehir = 'Istanbul'
limit 5


select * from simple_data
where sehir = 'Istanbul' and yas > 40
limit 5

select meslek, avg(aylik_gelir) as ortalama_gelir, avg(yas) as ortlama_yas
from simple_data
group by meslek
order by ortalama_gelir desc
limit 5


select * from simple_data
where meslek = 'Saraç'

update simple_data set meslek = 'Saraç'
where meslek = ''


delete from simple_data
where isim = 'Gençay'

select * from simple_data
where isim = 'Gençay'
limit 5

-- ISIN

select * from simple_data
where meslek in ('Doktor','Berber')


select * from simple_data
where meslek not in ('Doktor','Berber')
