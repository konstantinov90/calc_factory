function detailed_cmp()
clc
load('common1_2015-07-31.mat')
hd = HourData;
tdate = num2str(hd.Settings.InputData{strcmp(hd.Settings.InputData(:,1), 'targetdate'), 4});
y_ = tdate(1:4);
m_ = tdate(5:6);
d_ = tdate(7:8);

load(['common_' d_ m_ y_ '.mat'])

hour = 1;
tab = 'Lines';

oldtab = HourData.(tab){hour}.InputData;
newtab = hd.(tab){hour}.InputData;
rows_cnt = max(size(newtab, 1), size(oldtab, 1));
cols_cnt = min(size(newtab, 2), size(oldtab, 2));

format bank;
for I = 1:rows_cnt
    if nnz(abs(oldtab(I,1:cols_cnt) - newtab(I,1:cols_cnt)) > 1e-10)
%         disp(oldtab(I,:));
%         disp(newtab(I,:));
        fprintf('row %i\n', I);
        break
    end
end