function correct_common(matfilename, settingsfilename, outputfolder)
load(matfilename)

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

for I = 1:length(vars)
    dummy = cell(1, 24);
    disp(vars{I});
    for hour = 1:24
        if HourData.(vars{I})(hour).InputData == 0
            HourData.(vars{I})(hour).InputData = zeros(0,6);
        end
        dummy{hour} = HourData.(vars{I})(hour);
    end
    HourData.(vars{I}) = dummy;
end

HourData.HourNumbers = num2cell(0:23);

load(settingsfilename)

settings_length = size(s1, 2);

settings = cell(settings_length, 5);
for I = 1:settings_length
    settings(I,:) = {s1{I} s2(I) s3{I} s4(I) s5(I)};
end

HourData.Settings.InputData = settings;

delete(settingsfilename)

Fuel = HourData.Fuel;

for hour = 1:24
    cnt = 0;
    HourData.GroupConstraints{hour}.Rges = cell(1, size(HourData.GroupConstraints{I}.InputData, 1));
    gr_c = HourData.GroupConstraintsRges{hour}.InputData;
    for i = 1:size(HourData.GroupConstraints{hour}.InputData(:,1),1)
        gr = HourData.GroupConstraints{hour}.InputData(i,1);
        cnt = cnt + 1;
        HourData.GroupConstraints{hour}.Rges{1, cnt} = struct('InputData', gr_c(gr_c(:,1) == gr, 2));
    end
end

save(matfilename, 'HourData', 'Fuel')

copyfile(matfilename, outputfolder);