function matlab_cmp ()
clc
load('common1_2015-07-31.mat')
hd = HourData;
tdate = num2str(hd.Settings.InputData{strcmp(hd.Settings.InputData(:,1), 'targetdate'), 4});
y_ = tdate(1:4);
m_ = tdate(5:6);
d_ = tdate(7:8);
if datenum(tdate, 'yyyymmdd') < datenum('01-Mar-2016')
    clear_nodes_price_zone_fixed = true;
else
    clear_nodes_price_zone_fixed = false;
end
if datenum(tdate, 'yyyymmdd') < datenum('23-Oct-2015')
    clear_demands_is_accepted = true;
else
    clear_demands_is_accepted = false;
end
if datenum(tdate, 'yyyymmdd') < datenum('01-May-2016')
    clear_impex_is_accepted = true;
else
    clear_impex_is_accepted = false;
end


load(['common_' d_ m_ y_ '.mat'])
vars = {'Lines' ...
    'Nodes' ...
    'Demands' ...
    'NodesPQ' ...
    'NodesPV' ...
    'NodesSW' ...    
    'Generators' ...
    'Supplies' ...
    'Shunts' ...
    'GroupConstraints' ...
    'GroupConstraintsRges' ...
    'Sections' ...
    'SectionLines' ...
    'SectionsImpex' ...
    'SectionLinesImpex' ...
    'PriceZoneDemands' ...
    'ImpexBids' ...
    };

for hour = 1:24
    hd.SectionLines{hour}.InputData = sortrows(hd.SectionLines{hour}.InputData,[2,3,1,5]);
    HourData.SectionLines{hour}.InputData = sortrows(HourData.SectionLines{hour}.InputData,[2,3,1,5]);
    hd.SectionLinesImpex{hour}.InputData = sortrows(hd.SectionLinesImpex{hour}.InputData,[2,3,1,5]);
    HourData.SectionLinesImpex{hour}.InputData = sortrows(HourData.SectionLinesImpex{hour}.InputData,[2,3,1,5]);
    hd.Supplies{hour}.InputData = hd.Supplies{hour}.InputData(hd.Supplies{hour}.InputData(:,2) > 1e-10,:);
    HourData.Supplies{hour}.InputData = HourData.Supplies{hour}.InputData(HourData.Supplies{hour}.InputData(:,2) > 1e-10,:);
    
    hd.Demands{hour}.InputData = hd.Demands{hour}.InputData(hd.Demands{hour}.InputData(:,5) > 1e-10,:);
    for I = 2:size(hd.Demands{hour}.InputData, 1)
        N = hd.Demands{hour}.InputData(I-1, 1);
        M = hd.Demands{hour}.InputData(I, 1);
        if M - N > 1
            hd.Demands{hour}.InputData(I:end, 1) = hd.Demands{hour}.InputData(I:end, 1) - 1;
        end
    end
    HourData.Demands{hour}.InputData = HourData.Demands{hour}.InputData(HourData.Demands{hour}.InputData(:,5) > 1e-10,:);
    for I = 2:size(HourData.Demands{hour}.InputData, 1)
        N = HourData.Demands{hour}.InputData(I-1, 1);
        M = HourData.Demands{hour}.InputData(I, 1);
        if M - N > 1
            HourData.Demands{hour}.InputData(I:end, 1) = HourData.Demands{hour}.InputData(I:end, 1) - 1;
        end
    end
    
    HourData.PriceZoneDemands{hour}.InputData = round(HourData.PriceZoneDemands{hour}.InputData * 1e5) / 1e5;
    hd.PriceZoneDemands{hour}.InputData = round(hd.PriceZoneDemands{hour}.InputData * 1e5) / 1e5;
    
    if clear_nodes_price_zone_fixed
        hd.Nodes{hour}.InputData(:,end) = [];
    end
    if clear_demands_is_accepted
        hd.Demands{hour}.InputData(:,end) = [];
    end
    if clear_impex_is_accepted
        hd.ImpexBids{hour}.InputData(:,end) = [];
    end
end


for i = 1:length(vars)
    disp(vars{i})
for hour = 1:24
    try
        M_new = eval(['size(hd.' vars{i} '{' num2str(hour) '}.InputData)']);
        M_old = eval(['size(HourData.' vars{i} '{' num2str(hour) '}.InputData)']);
        if (M_new ~= [0 0]) & (M_old ~= [0 0]) 
            N = eval(['nnz(abs(hd.' vars{i} '{' num2str(hour) '}.InputData ' ...
                '- HourData.' vars{i} '{' num2str(hour) '}.InputData) > 1e-10)']);
            if N
                fprintf('%s - %i = %i\n', vars{i}, hour, N)
            end
        elseif M_new ~= M_old
            fprintf('%s has %i size!', vars{i}, M_new)
        end
    catch e
        disp(hour)
        throw(e)
    end
end
end

vars = {'SectionRegions' ...
        'GeneratorsDataLastHour' ...
        'Fuel' ...
       };
for i = 1:length(vars)
    disp(vars{i})
    N = eval(['nnz(abs(hd.' vars{i} '.InputData ' ...
        '- HourData.' vars{i} '.InputData) > 1e-10)']);
    if N
        fprintf('%s - %i = %i\n', vars{i}, hour, N)
    end
end

