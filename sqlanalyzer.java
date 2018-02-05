
import java.io.*;
import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

class sqlanalyzer
{


public static void main(String args[]) throws Exception
   {
	   int tokencount;
	   
	   
	   
	   
	   FileWriter fstream = new FileWriter("out.dat");
	   BufferedWriter out = new BufferedWriter(fstream);
		  
	   String[] search_words = { "P1V","P1D"};
	   ArrayList<String> input_files = new ArrayList<String>();
	   ArrayList<String> output_table = new ArrayList<String>();
	   ArrayList<String> output_view = new ArrayList<String>();
	   Set<String> final_output_table = new HashSet<String>();
	   
		
	   ArrayList<String> alias_list_from_ctl_id = new ArrayList<String>();
	   ArrayList<Pattern> regex_pattern_to_fix_ctl_id_issue = new ArrayList<Pattern>();
	   ArrayList<Pattern> regex_pattern_to_find_main_table_from_alias = new ArrayList<Pattern>();
	   ArrayList<Pattern> regex_pattern_to_search_columns = new ArrayList<Pattern>();
	   ArrayList<String> list_of_columns = new ArrayList<String>();
	   //ArrayList<String> regex_pattern_ctl_id = new ArrayList<String>();
	   
	   
	   File dir = new File(".");
		File [] files = dir.listFiles(new FilenameFilter() {
		@Override
		public boolean accept(File dir, String name) {
			return name.endsWith(".txt");
		}
		});

		for (File xmlfile : files) {
			/*System.out.println(xmlfile);*/
			input_files.add(xmlfile.toString().substring(2));
		}
		
		//For replacing the "RELPACE VIEW P1V" Issue
		for (int f=0;f<input_files.size();f++)
	   {
				
				String s,putData;
				String allData = new String();
				String tmpFileName = "temp.dat";
				
				FileReader fr=new FileReader(input_files.get(f));
				BufferedReader br=new BufferedReader(fr);
				
				FileWriter fstream0 = new FileWriter(tmpFileName);
				BufferedWriter out0 = new BufferedWriter(fstream0);
						
				
			   while ((s=br.readLine())!=null)
			   {
					putData = s.replaceAll("^.*?(?i)REPLACE.*?(?i)VIEW.*?(?i)P1V", "REPLACE VIEW P1V");
					putData = putData.replaceAll("\\s*\\.\\s*", ".");
					putData = putData.replaceAll("\\s+(?i)AS\\s*", " AS ");
					putData = putData.replaceAll("--.*?(?i)$", "");//to remove line after -- comment text
					allData = allData.concat(putData+"\n"); // to add new line character after above processing
					allData = allData.replaceAll("(?s)/\\*.*?\\*/","");//to replace the /**/ comment text
                    //out0.write(putData+"\n");
					
				}
				out0.write(allData);
				fr.close();
				out0.close();
				
			
				// Once everything is complete, delete old file..
				File oldFile = new File(input_files.get(f));
				oldFile.delete();

				// And rename tmp file's name to old file name
				File newFile = new File(tmpFileName);
				newFile.renameTo(oldFile);
		}
		
		//to create the ctl_id list from the view
		for (int f=0;f<input_files.size();f++)
		{
				
				String s;
				String ctl_id_data = new String();
				String tmpFileName_fullline = "temp_fulline.dat";
				String tmpFileName_matchedgroup = "temp_matchedgroup.dat";
				String tempdata_fullline = new String();
				String tempdata_matchedgroup = new String();
				
				//for selecting table alias having required ctl_id
				Pattern pattern = Pattern.compile("(\\w+)\\.");
				Matcher matcher = pattern.matcher(""); // Declare and initialize a matcher				
				
				FileReader fr=new FileReader(input_files.get(f));
				BufferedReader br=new BufferedReader(fr);
				
				FileWriter fstream_fullline = new FileWriter(tmpFileName_fullline);
				BufferedWriter out_fullline = new BufferedWriter(fstream_fullline);
				
				FileWriter fstream_matchedgroup = new FileWriter(tmpFileName_matchedgroup);
				BufferedWriter out_matchedgroup = new BufferedWriter(fstream_matchedgroup);

				while ((s=br.readLine())!=null)
			   {
				   if(s.contains("'005'"))
					{	
						ctl_id_data = s;
						//System.out.println("CTL_ID "+ctl_id_data);
						
						
						matcher.reset(ctl_id_data);//reset matcher with required pattern
						
						if (matcher.find())
						{
							alias_list_from_ctl_id.add(matcher.group(1));
							//System.out.println(matcher.group(1));
							tempdata_fullline = tempdata_fullline.concat(s+"\n");
							tempdata_matchedgroup = tempdata_matchedgroup.concat(matcher.group(0)+"\n");
							//System.out.println(matcher.group(0));
							//System.out.println(s);
							
						}
					}
				   
			   }
			   
			   out_fullline.write(tempdata_fullline);
				out_fullline.close();
				
				out_matchedgroup.write(tempdata_matchedgroup);
				out_matchedgroup.close();
				
				//loop to print alias of ctl_id & generate the search pattern text for extracting the data 
				for(int j=0;j<alias_list_from_ctl_id.size();j++)
				{					
						//System.out.println(alias_list_from_ctl_id.get(j));
						
						regex_pattern_to_fix_ctl_id_issue.add(Pattern.compile(
						"(\\w+).CTL_ID.*?(?i)=.*?(?i)"+alias_list_from_ctl_id.get(j)
						+".*?(?i)\\..*?(?i)CTL_ID|"+alias_list_from_ctl_id.get(j)
						+".*?(?i)\\..*?(?i)CTL_ID.*?(?i)=.*?(?i)(\\w+).CTL_ID"));
						
						/*System.out.println(
						"(\\w+).CTL_ID.*?(?i)=.*?(?i)"+alias_list_from_ctl_id.get(j)
						+".*?(?i)\\..*?(?i)CTL_ID|"+alias_list_from_ctl_id.get(j)
						+".*?(?i)\\..*?(?i)CTL_ID.*?(?i)=.*?(?i)(\\w+).CTL_ID");*/
															
				}

		}
		
		
		System.out.println("xxx.ctl_id = yyy.ctl_id : "+input_files.size());
		//For adding xxx.ctl_id = yyy.ctl_id cases into list of alias_list_from_ctl_id
		for (int f=0;f<input_files.size();f++)
		{
				
				String s;
				String column_search_pattern_list = new String();
				String SearchPatternListFileName = "SearchPatternList.dat";
				String tmpFileName_SearchPatternListMainTableFromAlias = "SearchPatternList_MainTable_AliasName.dat";				
				String tempdata_SearchPatternMainTable = new String();					
				
				FileReader fr=new FileReader(input_files.get(f));
				BufferedReader br=new BufferedReader(fr);

				FileWriter fstream1 = new FileWriter(SearchPatternListFileName);
				BufferedWriter out1 = new BufferedWriter(fstream1);
				
				FileWriter fstream_SearchPatternListMainTableFromAlias = new FileWriter(tmpFileName_SearchPatternListMainTableFromAlias);
				BufferedWriter out_SearchPatternListMainTableFromAlias = new BufferedWriter(fstream_SearchPatternListMainTableFromAlias);
				
			   while ((s=br.readLine())!=null)
			   {
					
					for(int j=0;j<regex_pattern_to_fix_ctl_id_issue.size();j++)
					{
						
						
						Matcher matcher = regex_pattern_to_fix_ctl_id_issue.get(j).matcher(s); // Declare and initialize a matchermatcher.reset(ctl_id_data);//reset matcher with required pattern
						
						while(matcher.find())
						{
							if(matcher.group(1) != null)
							{
								//System.out.println(matcher.group(1));
								alias_list_from_ctl_id.add(matcher.group(1));
							}
							else{

								//System.out.println(matcher.group(2));
								alias_list_from_ctl_id.add(matcher.group(2));
							}
						}
					
					}
					

				}
				
				fr.close();
				
				//to remove the duplicates
				//Set<String> hs = new HashSet<>();
				//hs.addAll(alias_list_from_ctl_id);
				//alias_list_from_ctl_id.clear();
				//alias_list_from_ctl_id.addAll(hs);
				
								
				System.out.println("Total alias : "+alias_list_from_ctl_id.size());
				for(int j=0;j<alias_list_from_ctl_id.size();j++)
				{					
						System.out.println(alias_list_from_ctl_id.get(j));
				
				}
				
				//loop to print alias of ctl_id & generate the search pattern text for extracting the data 
				for(int j=0;j<alias_list_from_ctl_id.size();j++)
				{					
						//System.out.println(alias_list_from_ctl_id.get(j));
						
						regex_pattern_to_search_columns.add(Pattern.compile("("+alias_list_from_ctl_id.get(j)+"\\.\\w+[\\s]*=.*?(?i)$)|("
						+alias_list_from_ctl_id.get(j)+"\\.\\w+[\\s]*IN.*?(?i).\\)$)|("
						+alias_list_from_ctl_id.get(j)+"\\.\\w+[\\s]*LIKE.*?(?i).\\)$)|("
						+alias_list_from_ctl_id.get(j)+"\\.\\w+)",Pattern.MULTILINE));
						
						//to search the main table from alias name
						regex_pattern_to_find_main_table_from_alias.add(Pattern.compile(
						"(\\w+)[\\s]+AS[\\s]+("+alias_list_from_ctl_id.get(j)+")[\\s]+"));	
						
						column_search_pattern_list = column_search_pattern_list.concat(regex_pattern_to_search_columns.get(j)+"\n");
						
						tempdata_SearchPatternMainTable = tempdata_SearchPatternMainTable.concat(regex_pattern_to_find_main_table_from_alias.get(j)+"\n");
				}
				
				out1.write(column_search_pattern_list);
				out1.close();
				
				out_SearchPatternListMainTableFromAlias.write(tempdata_SearchPatternMainTable);
				out_SearchPatternListMainTableFromAlias.close();
				
				
		}
		
		System.out.println("------------Search COLUMNS---------------");
		//For searching the required columns using regex pattern
		for (int f=0;f<input_files.size();f++)
	   {
				
				String s,putData;
				String ctl_id_data = new String();
				String allData_for_file_output_2 = new String();
				String allData_for_file_output_3 = new String();
				String allData_for_searching = new String();
				String allData_temp = new String();
				String tmpFileName2 = "List_of_Columns.dat";
				String tmpFileName3 = "List_of_Table_&_AliasName.csv";
				String tmpFileName4 = "List_of_UnwantedColumns.dat";

				FileReader fr=new FileReader(input_files.get(f));
				BufferedReader br=new BufferedReader(fr);
				
				FileWriter fstream2 = new FileWriter(tmpFileName2);
				BufferedWriter out2 = new BufferedWriter(fstream2);
				
				FileWriter fstream3 = new FileWriter(tmpFileName3);
				BufferedWriter out3 = new BufferedWriter(fstream3);
				
				FileWriter fstream4 = new FileWriter(tmpFileName4);
				BufferedWriter out4 = new BufferedWriter(fstream4);
				
				ArrayList<Pattern> regex_pattern_to_remove_unwanted_columns = new ArrayList<Pattern>();
				regex_pattern_to_remove_unwanted_columns.add(Pattern.compile("^.*?(?i)\\.ACCOUNT_NUM.*?(?i)$"));
				regex_pattern_to_remove_unwanted_columns.add(Pattern.compile("^.*?(?i)\\.ACCOUNT_MODIFIER_NUM.*?(?i)$"));
				regex_pattern_to_remove_unwanted_columns.add(Pattern.compile("^.*?(?i)\\.CTL_ID.*?(?i)$"));
				regex_pattern_to_remove_unwanted_columns.add(Pattern.compile("^.*?(?i)\\.START_DT.*?(?i)$"));
				regex_pattern_to_remove_unwanted_columns.add(Pattern.compile("^.*?(?i)\\.END_DT.*?(?i)$"));
				regex_pattern_to_remove_unwanted_columns.add(Pattern.compile("^.*?(?i)\\.AGMT_CTL_ID.*?(?i)$"));
				regex_pattern_to_remove_unwanted_columns.add(Pattern.compile("^.*?(?i)\\.AS_OF_DT.*?(?i)$"));
				regex_pattern_to_remove_unwanted_columns.add(Pattern.compile("^.*?(?i)\\.RECORD_DELETED_FLAG.*?(?i)$"));
				
				
			   while ((s=br.readLine())!=null)
			   {
					/*if(s.contains(".ACCOUNT_NUM") 
					   ||s.contains(".ACCOUNT_MODIFIER_NUM") 
					   ||s.contains(".CTL_ID") 
					   ||s.contains(".START_DT")
					   ||s.contains(".END_DT")
					   ||s.contains(".AGMT_CTL_ID")
					   ||s.contains(".AS_OF_DT")
					   ||s.contains(".RECORD_DELETED_FLAG = 0"))
					   {}
					else{*/					
					allData_for_searching = allData_for_searching.concat(s+"\n");//}
				}
				
					//loop to search the required columns 
					for(int j=0;j<regex_pattern_to_search_columns.size();j++)
					{
						//matcher.reset(s);//reset matcher with required pattern
						Matcher matcher = regex_pattern_to_search_columns.get(j).matcher(allData_for_searching);
						
						while(matcher.find())
						{
								list_of_columns.add(matcher.group());
								//System.out.println(matcher.group(1));
								if(matcher.group().toUpperCase().contains(".ACCOUNT_NUM") 
								|| matcher.group().toUpperCase().contains(".ACCOUNT_MODIFIER_NUM") 
								|| matcher.group().toUpperCase().contains(".CTL_ID") 
								|| matcher.group().toUpperCase().contains(".START_DT")
								|| matcher.group().toUpperCase().contains(".END_DT")
								|| matcher.group().toUpperCase().contains(".AGMT_CTL_ID")
								|| matcher.group().toUpperCase().contains(".AS_OF_DT")
								|| matcher.group().toUpperCase().contains(".RECORD_DELETED_FLAG = 0"))
								{
									//System.out.println("break");
									//break;
									allData_temp = allData_temp.concat(matcher.group()+"\n");
									
								}
								else{
																	
								allData_for_file_output_2 = allData_for_file_output_2.concat(matcher.group()+"\n"); // to add new line character after above processing
								}
							
						}

					}
					
					
				out2.write(allData_for_file_output_2);
				out4.write(allData_temp);
				
				fr.close();
				out2.close();
				out4.close();
				
				
				
				
					//loop to search the main table from alias 
					for(int j=0;j<regex_pattern_to_find_main_table_from_alias.size();j++)
					{
						//matcher.reset(s);//reset matcher with required pattern
						Matcher matcher = regex_pattern_to_find_main_table_from_alias.get(j).matcher(allData_for_searching);
						
						while(matcher.find())
						{
								//list_of_columns.add(matcher.group());
								System.out.println(matcher.group(1)+" "+matcher.group(2));
								allData_for_file_output_3 = allData_for_file_output_3.concat(matcher.group(2)+","+matcher.group(1)+"\n"); // to add new line character after above processing
							
						}

					}
					
					out3.write(allData_for_file_output_3);
					out3.close();
				
		}
		
		
	    
	   //for listing the views and tables
	   for (int f=0;f<input_files.size();f++)
	   {
	   int count_tables = 0;
	   int count_views = 0;
	   
	   
		   for (int i=0;i < search_words.length;i++) 
		   {
				
				
				String s;
				int linecount=0;
				String line;
				String words[]=new String[500];
				
				FileReader fr=new FileReader(input_files.get(f));
				BufferedReader br=new BufferedReader(fr);
				
				
			   while ((s=br.readLine())!=null)
			   {
				  //linecount++;
				  
				  int indexfound=s.indexOf(search_words[i]);
							  
				  if (indexfound > -1)
				  {
					  /*System.out.println("\n");*/
					  /*System.out.println("Word was found at position::" +indexfound+ "::on line"+linecount);*/
					  /*System.out.println("\n");*/
					  /*out.write ("\n Word was found at position:: "+indexfound+"::on line"+linecount);*/
						  
					  line=s;
										  
					  /*System.out.println(line);*/
					  /*out.write (line);*/
										  
					  int idx=0;
					  
					  StringTokenizer st= new StringTokenizer(line);
					  tokencount= st.countTokens();
					  /*System.out.println("\n");*/
					  /*System.out.println("Number of tokens found" +tokencount);*/
					  /*System.out.println("\n");*/		
					  /*out.write ("\n Number of tokens found "+tokencount+"\n");*/
					  
					  for (idx=0;idx < tokencount;idx++)
					  {
							words[idx]=st.nextToken();
							
							if(words[idx].contains("--"))//To skip single line comment
							{
								/*System.out.println(words[idx]);*/
								break;
							}
							else{
							
									if(words[idx].contains(search_words[i]))
									{
										/*System.out.println(line);*/
										
										if(line.contains("REPLACE VIEW "+words[idx]))
										{
											output_view.add(count_views,words[idx]);
											count_views++;
											/*System.out.println("REPLACE VIEW "+search_words[i]);*/
										}
										else
										{
											output_table.add(count_tables,words[idx]);
											count_tables++;
											//System.out.println(words[idx]);
											
										}
										
									}
							}
							
					  }
					}

				}
				fr.close();
			}
			
			final_output_table.addAll(output_table);
			output_table.clear();
			output_table.addAll(final_output_table);
			
			/* Sort statement*/
		   Collections.sort(output_table);
			
			for(int j=0;j<output_view.size();j++)
			{
				for(int i=0;i<output_table.size();i++)
				{
					out.write(output_view.get(j)+"|"+output_table.get(i)+"\n");
				}
			}
			/*for(String counter: output_table){
				System.out.println(counter);
			}*/
			
			/*for(String counter: output_view){
				System.out.println(counter);
			}*/
			output_table.clear();
			output_view.clear();
			final_output_table.clear();
		}
					
		out.close();
		System.out.println("Done! Check out.txt");
   }

}