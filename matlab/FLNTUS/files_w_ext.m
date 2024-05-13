function [files_ext,files_all]=files_w_ext(dpath,ext);
% this function will find all the files in a folder
% that have a particular extension

% determine default values
if nargin<=1, ext='m'; end
if nargin<=0, dpath=pwd; end

% get the directory structure
s=dir(dpath);

% set counters to zero
acnt=0;
ecnt=0;

% loop thru by the number of items in the folder
for i=1:size(s,1)
  if ~s(i).isdir   % check to see if the item is not a folder
	  acnt=acnt+1;
		files_all{acnt,1}=s(i).name;  % put the name of the item in the cell array
		j=find(s(i).name=='.');       % find the start of the extension
		if ~isempty(j)
		  if strcmp(s(i).name(j+1:end),ext)  % is the extension you want in there?
			  ecnt=ecnt+1;
			  files_ext{ecnt,1}=s(i).name;  % put the item in the cell array
			end
		end
	end
end

