c:`event_id`app_id`is_installed`is_active;
colStr:"SSII";
.Q.fs[{`appe insert flip c!(colStr;",")0:x}]`:app_events.csv;
cev:count each group appe[`event_id];
capp:count each group appe[`app_id];
st:(+\)value cev;
phew:sum each st _ appe[`is_active];

tmp:sum appe[where appe[`event_id]=`2][`is_active];
phew:tmp,phew;
phew:phew[til (-1+count phew)];
t:(key cev),'phew;
pcisactiv:(key cev),'phew%value cev;

c:`label_id`category
colStr:"SS";
.Q.fs[{`lc insert flip c!(colStr;",")0:x}]`:label_categories.csv;

c:`app_id`label_id
colStr:"SS";
.Q.fs[{`al insert flip c!(colStr;",")0:x}]`:app_labels.csv;

alc:ej[`label_id;al;lc];

calc:count each group alc[`category];

mfcat:(key calc)[where (value calc)>2000];

mfapp:(key capp)[where (value capp)>25000];

h:count each group alc[`label_id];
mflabels:(key h)[where (value h)>2000];
/ start filtering
t:alc[where alc[`category] in mfcat];

t: t[where t[`label_id] in mflabels];

cta:count each group t[`app_id]

mfappid:((key cta)[where (value cta)>5]),'0.8;

pc:([]event_id:pcisactiv[;0];active_score:pcisactiv[;1]);
jappepc:ej[`event_id;appe;pc];

jappepc:delete is_active,is_installed  from jappepc;

mfappid:([]app_id:mfappid[;0];app_id_score:mfappid[;1]);
l1:distinct jappepc[`app_id];
l2:mfappid[`app_id];

t:(count jappepc)#(0.8);
t[where not l1 in l2]:0.1;
appidscore:([]app_id_score:t);
jappepc:jappepc,'appidscore;
jappepc:delete app_id from jappepc;
gjappepc:select by event_id from jappepc;

c:`event_id`device_id`timestamp`longitude`latitude
colStr:"SSSSS"
.Q.fs[{`ev insert flip c!(colStr;",")0:x}]`:events.csv;
ev:delete from ev where ev[`event_id]=`;
l1:distinct ev[`event_id];
l2:key gjappepc;
l2:l2[`event_id];
diff:l1[where not l1 in l2];
vg:value gjappepc;
cd:count diff;
cg:count vg;
master:til cg;
flist:();
data:vg;
sampler:{[master;c]smpl:(floor (count master)%(1.2))?(count master);
	$[0=count flist;
	flist::data[smpl];
	flist::flist,data[smpl]];
	m:where not master in smpl;
	$[c>count flist;sampler[m;c];
	flist::flist[til c]]}

evsmpl:sampler[master;count data];
diff:([]event_id:diff);
evs:diff,'evsmpl;
evs:delete from evs where evs[`event_id]=`
consevs:gjappepc,select by event_id from evs;
jevconsevs:ej[`event_id;ev;consevs];

c:`device_id`gender`age`group
colStr:"SSSS"
.Q.fs[{`ga insert flip c!(colStr;",")0:x}]`:gender_age_train.csv;
ga:delete from ga where ga[`device_id]=`;	

l1:distinct ga[`device_id];
l2:distinct jevconsevs[`device_id];
l1diff:l1[where not l1 in l2];
l2diff:l2[where not l2 in l1];
/ sample from jevconsevs for imputing
cj:count jevconsevs;

tmpga:ga;
tmpcmn:tmpga[where l1 in l2];
tmpga:delete from tmpga where device_id in tmpcmn[`device_id];

flist:();
data:jevconsevs[where jevconsevs[`device_id] in l2diff]; / sample only from device_ids of jevconsevs that don't exist in ga 
master:til count data;
devidsmpl:sampler[master;(count l1diff)]; 
devidsmpl:delete device_id,latitude,longitude,timestamp from devidsmpl;
tmpdevidtbl:([]device_id:l1diff);
tmpdevidtbl:tmpdevidtbl,'devidsmpl;
tmpga:ej[`device_id;tmpga;tmpdevidtbl];

flist:();
data:jevconsevs[where tmpga[`device_id] in l1diff]; / sample only from device_ids of jevconsevs that don't exist in jevconsevs 
master:til count data;
devidsmpl:sampler[master;(count l2diff)]; 
devidsmpl:delete device_id,latitude,longitude,timestamp from devidsmpl;
tmpdevidtbl:([]device_id:l2diff);
tmpdevidtbl:tmpdevidtbl,'devidsmpl;
/tmpga:ej[`device_id;tmpga;tmpdevidtbl];
tmpga:tmpga uj tmpdevidtbl; / fills in null for no values

data:jevconsevs[where jevconsevs[`device_id] in tmpcmn[`device_id]]; /common rows
master:til count data;
flist:();
cmndevidsmpl:sampler[master;(count tmpcmn)]; / get me (count tmpcmn -23309) from all device_ids of jevconsevs that are common to both l1 and l2
cmndevidsmpl:delete device_id,latitude,longitude,timestamp from cmndevidsmpl;
cmntmpdevidtbl:([]device_id:tmpcmn[`device_id]);
cmntmpdevidtbl:cmntmpdevidtbl,'cmndevidsmpl;
tmpcmn:ej[`device_id;tmpcmn;cmntmpdevidtbl];
fga:tmpcmn,tmpga; /finally !


c:`device_id`phone_brand`device_model
colStr:"SSS"
.Q.fs[{`pbdm insert flip c!(colStr;",")0:x}]`:phone_brand_device_model.csv;
pbdm:distinct pbdm;
pbdm:delete from pbdm where pbdm[`device_id]=`;
final:jgapbdm:ej[`device_id;fga;pbdm];


