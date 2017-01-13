function save_mat_file(filename, data, var_name)
disp(var_name);
    if ~iscell(data)
        data = {data};
        var_name = {var_name};
    end
    disp(data)
    for I = 1:length(data) 
        if isfield(data{I}, 'Settings')
            data{I}.Settings.InputData = vertcat(data{I}.Settings.InputData{:});
        end
        cmd = sprintf('%s = data{%i};', var_name{I}, I);
        disp(cmd)
        eval(cmd);
%     eval(['disp(' var_name ')']);
    end
    cmd = sprintf('save(''%s'', %s)', filename, get_var_names(var_name));
    disp(cmd);
    eval(cmd);
%     save(filename, var_name);
end

function nms = get_var_names(var)
    if iscell(var)
        disp(sprintf('''%s'', ', var{1:end-1}))
        if length(var) > 1
            nms = [sprintf('''%s'', ', var{1:end-1}) sprintf('''%s''', var{end})];
        else
            nms = sprintf('''%s''', var{1});
        end
    else
        nms = sprintf('''%s''', var);
    end
end