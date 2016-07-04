/************************************************************
	Project#1:	CLP & DDL
 ************************************************************/

#include "db.h"
#include <stdio.h>
#include <string.h>
#include <string>
#include <stdlib.h>
#include <ctype.h>
#include <sys/stat.h>
#include <sys/types.h>

int main(int argc, char** argv)
{
	int rc = 0;
	token_list *tok_list=NULL, *tok_ptr=NULL, *tmp_tok_ptr=NULL;

	if ((argc != 2) || (strlen(argv[1]) == 0))
	{
		printf("Usage: db \"command statement\"");
		return 1;
	}

	rc = initialize_tpd_list();

    if (rc)
 	{
		printf("\nError in initialize_tpd_list().\nrc = %d\n", rc);
    }
	else
	{
    	rc = get_token(argv[1], &tok_list);

		/* Test code */
		tok_ptr = tok_list;
		// printf("%16s \tClass \t Value\n", "String");
		while (tok_ptr != NULL)
		{
			// printf("%16s \t%d \t %d\n",tok_ptr->tok_string, tok_ptr->tok_class, tok_ptr->tok_value);
			tok_ptr = tok_ptr->next;
		}
    
		if (!rc)
		{
			rc = do_semantic(tok_list);
		}

		if (rc)
		{
			tok_ptr = tok_list;
			while (tok_ptr != NULL)
			{
				if ((tok_ptr->tok_class == error) ||
					  (tok_ptr->tok_value == INVALID))
				{
					printf("\nError in the string: %s\n", tok_ptr->tok_string);
					printf("rc=%d\n", rc);
					break;
				}
				tok_ptr = tok_ptr->next;
			}
		}

    /* Whether the token list is valid or not, we need to free the memory */
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
	      tmp_tok_ptr = tok_ptr->next;
	      free(tok_ptr);
	      tok_ptr=tmp_tok_ptr;
		}
	}

	return rc;
}

/************************************************************* 
	This is a lexical analyzer for simple SQL statements
 *************************************************************/
int get_token(char* command, token_list** tok_list)
{
	int rc=0,i,j;
	char *start, *cur, temp_string[MAX_TOK_LEN];
	bool done = false;
	
	start = cur = command;
	while (!done)
	{
		bool found_keyword = false;

		/* This is the TOP Level for each token */
	  memset ((void*)temp_string, '\0', MAX_TOK_LEN);
		i = 0;

		/* Get rid of all the leading blanks */
		while (*cur == ' ')
			cur++;

		if (cur && isalpha(*cur))
		{
			// find valid identifier
			int t_class;
			do 
			{
				temp_string[i++] = *cur++;
			}
			while ((isalnum(*cur)) || (*cur == '_'));

			if (!(strchr(STRING_BREAK, *cur)))
			{
				/* If the next char following the keyword or identifier
				   is not a blank, (, ), or a comma, then append this
					 character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else
			{

				// We have an identifier with at least 1 character
				// Now check if this ident is a keyword
				for (j = 0, found_keyword = false; j < TOTAL_KEYWORDS_PLUS_TYPE_NAMES; j++)
				{
					if ((stricmp(keyword_table[j], temp_string) == 0))
					{
						found_keyword = true;
						break;
					}
				}

				if (found_keyword)
				{
				  if (KEYWORD_OFFSET+j < K_CREATE)
						t_class = type_name;
					else if (KEYWORD_OFFSET+j >= F_SUM)
            t_class = function_name;
          else
					  t_class = keyword;

					add_to_list(tok_list, temp_string, t_class, KEYWORD_OFFSET+j);
				}
				else
				{
					if (strlen(temp_string) <= MAX_IDENT_LEN)
					  add_to_list(tok_list, temp_string, identifier, IDENT);
					else
					{
						add_to_list(tok_list, temp_string, error, INVALID);
						rc = INVALID;
						done = true;
					}
				}

				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else if (isdigit(*cur))
		{
			// find valid number
			do 
			{
				temp_string[i++] = *cur++;
			}
			while (isdigit(*cur));

			if (!(strchr(NUMBER_BREAK, *cur)))
			{
				/* If the next char following the keyword or identifier
				   is not a blank or a ), then append this
					 character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else
			{
				add_to_list(tok_list, temp_string, constant, INT_LITERAL);

				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else if ((*cur == '(') || (*cur == ')') || (*cur == ',') || (*cur == '*')
		         || (*cur == '=') || (*cur == '<') || (*cur == '>'))
		{
			/* Catch all the symbols here. Note: no look ahead here. */
			int t_value;
			switch (*cur)
			{
				case '(' : t_value = S_LEFT_PAREN; break;
				case ')' : t_value = S_RIGHT_PAREN; break;
				case ',' : t_value = S_COMMA; break;
				case '*' : t_value = S_STAR; break;
				case '=' : t_value = S_EQUAL; break;
				case '<' : t_value = S_LESS; break;
				case '>' : t_value = S_GREATER; break;
			}

			temp_string[i++] = *cur++;

			add_to_list(tok_list, temp_string, symbol, t_value);

			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
		}
    else if (*cur == '\'')
    {
      /* Find STRING_LITERRAL */
			int t_class;
      cur++;
			do 
			{
				temp_string[i++] = *cur++;
			}
			while ((*cur) && (*cur != '\''));

      temp_string[i] = '\0';

			if (!*cur)
			{
				/* If we reach the end of line */
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
      else /* must be a ' */
      {
        add_to_list(tok_list, temp_string, constant, STRING_LITERAL);
        cur++;
				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
        }
      }
    }
		else
		{
			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
			else
			{
				/* not a ident, number, or valid symbol */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
		}
	}
			
  return rc;
}

void add_to_list(token_list **tok_list, char *tmp, int t_class, int t_value)
{
	token_list *cur = *tok_list;
	token_list *ptr = NULL;

	// printf("%16s \t%d \t %d\n",tmp, t_class, t_value);

	ptr = (token_list*)calloc(1, sizeof(token_list));
	strcpy(ptr->tok_string, tmp);
	ptr->tok_class = t_class;
	ptr->tok_value = t_value;
	ptr->next = NULL;

  if (cur == NULL)
		*tok_list = ptr;
	else
	{
		while (cur->next != NULL)
			cur = cur->next;

		cur->next = ptr;
	}
	return;
}

int do_semantic(token_list *tok_list)
{
	int rc = 0, cur_cmd = INVALID_STATEMENT;
	bool unique = false;
  	token_list *cur = tok_list;

	if ((cur->tok_value == K_CREATE) && ((cur->next != NULL) && (cur->next->tok_value == K_TABLE))){
		printf("CREATE TABLE statement\n");
		cur_cmd = CREATE_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_DROP) && ((cur->next != NULL) && (cur->next->tok_value == K_TABLE))){
		printf("DROP TABLE statement\n");
		cur_cmd = DROP_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_LIST) && ((cur->next != NULL) && (cur->next->tok_value == K_TABLE))){
		printf("LIST TABLE statement\n");
		cur_cmd = LIST_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_LIST) && ((cur->next != NULL) && (cur->next->tok_value == K_SCHEMA))){
		printf("LIST SCHEMA statement\n");
		cur_cmd = LIST_SCHEMA;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_SELECT) && ((cur->next != NULL) && (cur->next->tok_value == S_STAR ||
																		 cur->next->tok_value == IDENT ||
																		 cur->next->tok_value == F_AVG ||
																		 cur->next->tok_value == F_SUM ||
																		 cur->next->tok_value == F_COUNT))){
		printf("SELECT FROM TABLE statement\n");
		cur_cmd = SELECT;
		cur = cur->next;
	}
	else if ((cur->tok_value == K_INSERT) && ((cur->next != NULL) && (cur->next->tok_value == K_INTO))){
		printf("INSERT INTO statement\n");
		cur_cmd = INSERT;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_DELETE) && ((cur->next != NULL) && (cur->next->tok_value == K_FROM))){
		printf("DELETE FROM statement\n");
		cur_cmd = DELETE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_UPDATE) && ((cur->next != NULL) && (cur->next->next->tok_value == K_SET))){
		printf("UPDATE TABLE statement\n");
		cur_cmd = UPDATE;
		cur = cur->next;
	}
	else{
		printf("Invalid statement\n");
		rc = cur_cmd;
	}

	if (cur_cmd != INVALID_STATEMENT){
		switch(cur_cmd){
			case CREATE_TABLE:
						rc = sem_create_table(cur);
						break;
			case DROP_TABLE:
						rc = sem_drop_table(cur);
						break;
			case LIST_TABLE:
						rc = sem_list_tables();
						break;
			case LIST_SCHEMA:
						rc = sem_list_schema(cur);
						break;
			case SELECT:
						rc = sem_select_stmt(cur);
						// printf("Table: %s\n", cur->next->tok_string);
						// while(cur->next != NULL){
						// 	printf(" %s ", cur->tok_string);
						// 	cur = cur->next;
						// 
						// }
						// rc = sem_list_schema(cur);

						break;
			case INSERT:
						rc = sem_insert_stmt(cur);
						// printf("Table: %s\n", cur->next->tok_string);
						// while(cur->next != NULL){
						// 	printf("%s ", cur->tok_string);
						// 	cur = cur->next;

						// }
						// rc = sem_list_schema(cur);
						break;	
			case DELETE:
						// printf("%16s\n",cur->tok_string);
						rc = sem_delete_stmt(cur);
						break;		
			case UPDATE:
						// printf("%16s\n",cur->tok_string);
						rc = sem_update_stmt(cur);
						break;							
			default:
					; /* no action */
		}
	}
	
	return rc;
}
// ---------------------------------------------------------------------------------------------
int sem_create_table(token_list *t_list)
{
	int rc = 0;
	int rl = 0;
	token_list *cur;
	tpd_entry tab_entry;
	tpd_entry *new_entry = NULL;
	bool column_done = false;
	int cur_id = 0;
	cd_entry	col_entry[MAX_NUM_COL];


	memset(&tab_entry, '\0', sizeof(tpd_entry));
	cur = t_list;
	if ((cur->tok_class != keyword) && (cur->tok_class != identifier) && (cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if ((new_entry = get_tpd_from_list(cur->tok_string)) != NULL)
		{
			rc = DUPLICATE_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			strcpy(tab_entry.table_name, cur->tok_string);
			cur = cur->next;
			if (cur->tok_value != S_LEFT_PAREN)
			{
				//Error
				rc = INVALID_TABLE_DEFINITION;
				cur->tok_value = INVALID;
			}
			else
			{
				memset(&col_entry, '\0', (MAX_NUM_COL * sizeof(cd_entry)));

				/* Now build a set of column entries */
				cur = cur->next;
				do
				{
					if ((cur->tok_class != keyword) && (cur->tok_class != identifier) && (cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_COLUMN_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						int i;
						for(i = 0; i < cur_id; i++)
						{
              				/* make column name case sensitive */
							if (strcmp(col_entry[i].col_name, cur->tok_string)==0)
							{
								rc = DUPLICATE_COLUMN_NAME;
								cur->tok_value = INVALID;
								break;
							}
						}

						if (!rc)
						{
							strcpy(col_entry[cur_id].col_name, cur->tok_string);
							col_entry[cur_id].col_id = cur_id;
							col_entry[cur_id].not_null = false;    /* set default */

							cur = cur->next;
							if (cur->tok_class != type_name)
							{
								// Error
								rc = INVALID_TYPE_NAME;
								cur->tok_value = INVALID;
							}
							else
							{
                				/* Set the column type here, int or char */
								col_entry[cur_id].col_type = cur->tok_value;
								cur = cur->next;
		
								if (col_entry[cur_id].col_type == T_INT)
								{
									if ((cur->tok_value != S_COMMA) && (cur->tok_value != K_NOT) && (cur->tok_value != S_RIGHT_PAREN))
									{
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
								  	else
									{
										col_entry[cur_id].col_len = sizeof(int);
										
										if ((cur->tok_value == K_NOT) && (cur->next->tok_value != K_NULL))
										{
											rc = INVALID_COLUMN_DEFINITION;
											cur->tok_value = INVALID;
										}	
										else if ((cur->tok_value == K_NOT) && (cur->next->tok_value == K_NULL))
										{					
											col_entry[cur_id].not_null = true;
											cur = cur->next->next;
										}
	
										if (!rc)
										{
											/* I must have either a comma or right paren */
											if ((cur->tok_value != S_RIGHT_PAREN) &&
												  (cur->tok_value != S_COMMA))
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
	                  						{
												if (cur->tok_value == S_RIGHT_PAREN)
												{
 													column_done = true;
												}
												cur = cur->next;
											}
										}
									}
								}   // end of S_INT processing
								else
								{
									// It must be char()
									if (cur->tok_value != S_LEFT_PAREN)
									{
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
									else
									{
										/* Enter char(n) processing */
										cur = cur->next;
		
										if (cur->tok_value != INT_LITERAL)
										{
											rc = INVALID_COLUMN_LENGTH;
											cur->tok_value = INVALID;
										}
										else
										{
											/* Got a valid integer - convert */
											col_entry[cur_id].col_len = atoi(cur->tok_string);
											cur = cur->next;
											
											if (cur->tok_value != S_RIGHT_PAREN)
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
											{
												cur = cur->next;
						
												if ((cur->tok_value != S_COMMA) &&
														(cur->tok_value != K_NOT) &&
														(cur->tok_value != S_RIGHT_PAREN))
												{
													rc = INVALID_COLUMN_DEFINITION;
													cur->tok_value = INVALID;
												}
												else
												{
													if ((cur->tok_value == K_NOT) && (cur->next->tok_value != K_NULL))
													{
														rc = INVALID_COLUMN_DEFINITION;
														cur->tok_value = INVALID;
													}
													else if ((cur->tok_value == K_NOT) && (cur->next->tok_value == K_NULL))
													{					
														col_entry[cur_id].not_null = true;
														cur = cur->next->next;
													}
		
													if (!rc)
													{
														/* I must have either a comma or right paren */
														if ((cur->tok_value != S_RIGHT_PAREN) &&															  (cur->tok_value != S_COMMA))
														{
															rc = INVALID_COLUMN_DEFINITION;
															cur->tok_value = INVALID;
														}
														else
													 	{
															if (cur->tok_value == S_RIGHT_PAREN)
															{
																column_done = true;
															}
															cur = cur->next;
														}
													}
												}
											}
										}	/* end char(n) processing */
									}
								} /* end char processing */
							}
						}  // duplicate column name
					} // invalid column name

					/* If rc=0, then get ready for the next column */
					if (!rc)
					{
						cur_id++;
					}

				} while ((rc == 0) && (!column_done));
	
				if ((column_done) && (cur->tok_value != EOC))
				{
					rc = INVALID_TABLE_DEFINITION;
					cur->tok_value = INVALID;
				}

				if (!rc)
				{
					/* Now finished building tpd and add it to the tpd list */
					tab_entry.num_columns = cur_id;
					tab_entry.tpd_size = sizeof(tpd_entry) + sizeof(cd_entry) *	tab_entry.num_columns;
				  	tab_entry.cd_offset = sizeof(tpd_entry);
					new_entry = (tpd_entry*)calloc(1, tab_entry.tpd_size);

					if (new_entry == NULL)
					{
						rc = MEMORY_ERROR;
					}
					else
					{
						memcpy((void*)new_entry, (void*)&tab_entry, sizeof(tpd_entry));
		
						memcpy((void*)((char*)new_entry + sizeof(tpd_entry)), (void*)col_entry, sizeof(cd_entry) * tab_entry.num_columns);
	
						rc = add_tpd_to_list(new_entry);



						free(new_entry);
					}
				}
			}
		}
	}
	// --------------------CREATE TABLE FILE--------------------
	if(!rc){
		printf("Creating Table File!\n");
		// printf("Table File: %s.tab\n", tab_entry.table_name);
		for(int i = 0; i < sizeof(cd_entry);i++){
			if(col_entry[i].col_type == 10 || col_entry[i].col_type == 11){
				// printf("Col: %s ", col_entry[i].col_name);
				// printf("Size: %d\n", col_entry[i].col_len);
				rl = rl + (col_entry[i].col_len) + 1;
			}			
		}
		printf("Total Col Size: %d\n", rl);
		rc = initialize_table_list(tab_entry.table_name,rl);
		// printf("Exit: %d\n", rc);

	}
	// ---------------------------------------------------------
  return rc;
}

int sem_drop_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;

	cur = t_list;
	if ((cur->tok_class != keyword) &&
		  (cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if (cur->next->tok_value != EOC)
		{
			rc = INVALID_STATEMENT;
			cur->next->tok_value = INVALID;
		}
		else
		{
			if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
			{
				rc = TABLE_NOT_EXIST;
				cur->tok_value = INVALID;
			}
			else
			{
				/* Found a valid tpd, drop it from tpd list */
				rc = drop_tpd_from_list(cur->tok_string);
				rc = delete_table_file(cur->tok_string);
			}
		}
	}

  return rc;
}

int sem_list_tables()
{
	int rc = 0;
	int num_tables = g_tpd_list->num_tables;
	tpd_entry *cur = &(g_tpd_list->tpd_start);

	if (num_tables == 0)
	{
		printf("\nThere are currently no tables defined\n");
	}
	else
	{
		printf("\nTable List\n");
		printf("*****************\n");
		while (num_tables-- > 0)
		{
			printf("%s\n", cur->table_name);
			if (num_tables > 0)
			{
				cur = (tpd_entry*)((char*)cur + cur->tpd_size);
			}
		}
		printf("****** End ******\n");
	}

  return rc;
}

int sem_list_schema(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	cd_entry  *col_entry = NULL;
	char tab_name[MAX_IDENT_LEN+1];
	char filename[MAX_IDENT_LEN+1];
	bool report = false;
	FILE *fhandle = NULL;
	int i = 0;

	cur = t_list;

	if (cur->tok_value != K_FOR)
    {
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
	}
	else
	{
		cur = cur->next;

		if ((cur->tok_class != keyword) && (cur->tok_class != identifier) && (cur->tok_class != type_name))
		{
			// Error
			rc = INVALID_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			memset(filename, '\0', MAX_IDENT_LEN+1);
			strcpy(tab_name, cur->tok_string);
			cur = cur->next;

			if (cur->tok_value != EOC)
			{
				if (cur->tok_value == K_TO)
				{
					cur = cur->next;
					
					if ((cur->tok_class != keyword) && (cur->tok_class != identifier) && (cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_REPORT_FILE_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						if (cur->next->tok_value != EOC)
						{
							rc = INVALID_STATEMENT;
							cur->next->tok_value = INVALID;
						}
						else
						{
							/* We have a valid file name */
							strcpy(filename, cur->tok_string);
							report = true;
						}
					}
				}
				else
				{ 
					/* Missing the TO keyword */
					rc = INVALID_STATEMENT;
					cur->tok_value = INVALID;
				}
			}

			if (!rc)
			{
				if ((tab_entry = get_tpd_from_list(tab_name)) == NULL)
				{
					rc = TABLE_NOT_EXIST;
					cur->tok_value = INVALID;
				}
				else
				{
					if (report)
					{
						if((fhandle = fopen(filename, "a+tc")) == NULL)
						{
							rc = FILE_OPEN_ERROR;
						}
					}

					if (!rc)
					{
						/* Find correct tpd, need to parse column and index information */

						/* First, write the tpd_entry information */
						printf("Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
						printf("Table Name               (table_name)  = %s\n", tab_entry->table_name);
						printf("Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
						printf("Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
            			printf("Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags); 

						if (report)
						{
							fprintf(fhandle, "Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
							fprintf(fhandle, "Table Name               (table_name)  = %s\n", tab_entry->table_name);
							fprintf(fhandle, "Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
							fprintf(fhandle, "Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
              				fprintf(fhandle, "Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags); 
						}

						/* Next, write the cd_entry information */
						for(i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
								i < tab_entry->num_columns; i++, col_entry++)
						{
							printf("Column Name   (col_name) = %s\n", col_entry->col_name);
							printf("Column Id     (col_id)   = %d\n", col_entry->col_id);
							printf("Column Type   (col_type) = %d\n", col_entry->col_type);
							printf("Column Length (col_len)  = %d\n", col_entry->col_len);
							printf("Not Null flag (not_null) = %d\n\n", col_entry->not_null);

							if (report)
							{
								fprintf(fhandle, "Column Name   (col_name) = %s\n", col_entry->col_name);
								fprintf(fhandle, "Column Id     (col_id)   = %d\n", col_entry->col_id);
								fprintf(fhandle, "Column Type   (col_type) = %d\n", col_entry->col_type);
								fprintf(fhandle, "Column Length (col_len)  = %d\n", col_entry->col_len);
								fprintf(fhandle, "Not Null Flag (not_null) = %d\n\n", col_entry->not_null);
							}
						}
	
						if (report)
						{
							fflush(fhandle);
							fclose(fhandle);
						}
					} // File open error							
				} // Table not exist
			} // no semantic errors
		} // Invalid table name
	} // Invalid statement

  return rc;
}



int sem_insert_stmt(token_list *t_list){
	int rc = 0;
	int i = 0;

	token_list *cur;
	tpd_entry *tab_entry = NULL;
	cd_entry  *col_entry = NULL;
	table_file_header *table = NULL;
	table_file_header *new_entry = NULL;

	int num_tables = g_tpd_list->num_tables;
	tab_entry = get_tpd_from_list(t_list->tok_string);


	cur = t_list;

	int len = strlen(tab_entry->table_name);
	char tab_name[MAX_IDENT_LEN+1];
	char *fname = new char[len+5];
	bool report = false;
	FILE *fhandle = NULL;
	struct _stat file_stat;
	

	if ((cur->tok_class != keyword) && (cur->tok_class != identifier) && (cur->tok_class != type_name)){
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else{

		cur = cur->next;

		strcpy(fname, tab_entry->table_name);
		fname[len] = '.';
		fname[len+1] = 't';
		fname[len+2] = 'a';
		fname[len+3] = 'b';
		fname[len+4] = '\0';

		// Open table file
	    if((fhandle = fopen(fname, "rbc")) == NULL)
		{
			printf("File Error");
			rc = FILE_OPEN_ERROR;
		} else {
			// Get Memory
			_fstat(_fileno(fhandle), &file_stat);
			// printf("Table File Size: %d\n", file_stat.st_size);
			table = (table_file_header*)calloc(1, file_stat.st_size);

			if (!table)
			{
				printf("Mem Error");
				rc = MEMORY_ERROR;
			} else {
			 
				// Load File into Memory
				fread(table, file_stat.st_size, 1, fhandle);
				fflush(fhandle);
				fclose(fhandle);
				// printf("S:%d / N:%d / L:%d / O:%d\n\n", table->file_size, table->num_records, table->record_size, table->record_offset );
				
				if (table->file_size != file_stat.st_size){
					printf("Size Error\n");
					rc = DBFILE_CORRUPTION;
				} else {
					cur = cur->next;
					if (cur->tok_value != S_LEFT_PAREN){
						//Add New Error
						rc = INVALID_TABLE_DEFINITION;
						cur->tok_value = INVALID;
					}else{
						new_entry = (table_file_header*)calloc(1,  table->file_size + table->record_size + (4 - (table->record_size % 4)));		
						memset(new_entry, 0, table->file_size + table->record_size );
						memcpy((void*)new_entry, (void*)table, table->file_size);
						new_entry->file_size = table->file_size + table->record_size;

						cur = cur->next;
						// printf("%s\n", cur->tok_string);
						bool done = false;
						col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);

						int pointer = sizeof(table_file_header) + (table->record_size * table->num_records);

						printf("\nINSERT INTO %s values(", tab_entry->table_name);
						int total = 0;
						while(!done){
							// IF int ELSE char
							if((cur->tok_value == INT_LITERAL && col_entry->col_type == 10 ) || cur->tok_value == K_NULL){
								// printf("Int\tCol:%d\tL:%d\n", col_entry->col_type, col_entry->col_len);

								int temp2 = col_entry->col_len;
								int *t2 = &temp2;
								memcpy((void*)((char*)new_entry + pointer), (void*)t2, sizeof(char));

								pointer = pointer + sizeof(char);

								if(strcmp(cur->tok_string, "NULL") == 0 ){
									if(col_entry->not_null == 1){
										printf("\n\nNot Null constraint exists for column name %s\n", col_entry->col_name);
										done = true;
										cur->tok_value = INVALID;
										rc=-380;
									}	
								}
								if(!rc){
									int temp = atoi(cur->tok_string);
									int *t = &temp;
									memcpy((void*)((char*)new_entry + pointer), (void*)t, sizeof(int));

									pointer = pointer + sizeof(int);
									total++;
									printf(" Int: %s", cur->tok_string);
								}

							} else if((cur->tok_value == 91 && col_entry->col_type == 11)  || cur->tok_value == K_NULL) {
								// printf("Char\tCol:%d\tL:%d\n", col_entry->col_type, 1);

								int temp2 = col_entry->col_len;
								int *t2 = &temp2;
								memcpy((void*)((char*)new_entry + pointer), (void*)t2, sizeof(char));
								pointer = pointer + sizeof(char);

								if(strcmp(cur->tok_string, "NULL") == 0 ){
									if(col_entry->not_null == 1){
										printf("\n\nNot Null constraint exists for column name %s\n", col_entry->col_name);
										done = true;
										cur->tok_value = INVALID;
										rc=-380;
									}	
								}
								if(!rc){
									memcpy((void*)((char*)new_entry + pointer), (void*)(cur->tok_string), col_entry->col_len);
									
									pointer = pointer + (col_entry->col_len);
									total++;
								}

								printf(" Char: \"%s\"", cur->tok_string);
							} else {
								printf("\n\nType missmatch\n");
								done = true;
								cur->tok_value = INVALID;
								// Type missmatch
								rc = -382;

							}

							if(!rc){
								if(cur->next->tok_value == S_COMMA){
									printf(",");		
									cur = cur->next->next;
								} else if(cur->next->tok_value == S_RIGHT_PAREN){
									printf(")");
									if(total < tab_entry->num_columns){
										printf("\n\nInvalid number of parameters.\n");
										cur->tok_value = INVALID;
										rc = -384;
									}
									cur = cur->next;
									done = true;
								} else {
									rc = INVALID_COLUMN_DEFINITION;
									cur->tok_value = INVALID;
									done = true;
								}
								col_entry++;
							}
						}
						if(!rc){
							printf(";\n");
							new_entry->num_records++;
							rc = write_to_table_file(new_entry, fname);
							free(new_entry);
							printf("\nINSERT Successful!\n");							
						} else {
							free(new_entry);
							printf("\nINSERT Failed!\n");	
						}
					}
				}
			}
		}
	}
	return rc;
}

int sem_select_stmt(token_list *t_list){
	int rc = 0;
	int i = 0;
	int j;
	int sel_col_offset = 0;

	token_list *cur, *cmd, *temp, *where_col_1, *where_col_2, *where_val_1, *where_val_2, *special_col, *order_col;
	where_val_1 = NULL;
	where_val_2 = NULL;
	where_col_1 = NULL;
	where_col_2 = NULL;
	special_col = NULL;
	order_col = NULL;
	tpd_entry *tab_entry = NULL;
	cd_entry  *col_entry = NULL;
	cd_entry  *temp_col = NULL;
	table_file_header *table = NULL;
	table_file_header *new_table = NULL;
	table_file_header *new_entry = NULL;

	// FUNCTIONS

	cmd = t_list;

	bool count_flag = false;
	bool avg_flag = false;
	bool sum_flag = false;
	float col_avg = 0;
	float col_sum = 0;
	bool fail = false;
	if(cmd->tok_value == F_COUNT){
		cmd = cmd->next->next; // (
		if(cmd->tok_value == IDENT || cmd->tok_value == S_STAR){
			count_flag = true;
		} else {
			printf("\nInvalid COUNT!\n");
			rc = INVALID_COLUMN_DEFINITION;
		}

	} else if(cmd->tok_value == F_AVG){
		cmd = cmd->next->next; // (
		if(cmd->tok_value == IDENT){
			avg_flag = true;
			special_col = cmd;
		} else {
			printf("\nInvalid AVG!\n");
			rc = INVALID_COLUMN_DEFINITION;
		}
		
	} else if(cmd->tok_value == F_SUM){
		cmd = cmd->next->next; // (	
		if(cmd->tok_value == IDENT){
			sum_flag = true;
			special_col = cmd;
		} else {
			printf("\nInvalid SUM!\n");
			rc = INVALID_COLUMN_DEFINITION;
		}
	} else {
		cmd = t_list;
	}

			//*
	// printf(cmd->tok_string);
	int num_sel_columns = 0;
	if(cmd->tok_value != S_STAR && (!count_flag || !avg_flag || !sum_flag)){
		while(cmd->next->tok_value != K_FROM){
			cmd = cmd->next;
			num_sel_columns++;
			if(cmd->tok_value == S_COMMA){
				cmd = cmd->next;
			}
			if(cmd->tok_value == S_STAR){
				rc = INVALID_COLUMN_DEFINITION;
				printf("\nINVALID COLUMN SELECT\n");
			}
		}
	} else if(count_flag || avg_flag || sum_flag){
		cmd = cmd->next;
	}
	// printf(cmd->tok_string);
	// if(cmd->tok_value == S_RIGHT_PAREN){
	// 	cmd = cmd->next; // )
	// }
	// printf(cmd->tok_string);
	cmd = cmd->next; //From
	cur = cmd;	//from
	cmd = cmd->next; // Table

	// GET WHERE content __________________________________________________________________________________
	int num_where = 0;
	bool where_flag = false;
	bool and_flag = false;
	bool or_flag = false;
	bool col_found;

	int where_col_i = 0;
	int * where_col_offset;
	where_col_offset = new int[2];
	where_col_offset[0] = 0;
	where_col_offset[1] = 0;

	int * where_len;
	where_len = new int[2];
	where_len[0] = 0;
	where_len[1] = 0;

	int where_ops_i = 0;
	int * where_ops;
	where_ops = new int[2];
	where_ops[0] = 0;
	where_ops[1] = 0;

	bool order_flag = false;
	bool order_desc = false;

	tab_entry = get_tpd_from_list(cur->next->tok_string);
	// printf("%s\n", cmd->tok_string);
	if(cmd->next->tok_value != EOC && cmd->next->tok_value == K_WHERE){
		where_flag = true;
		num_where++;
		cmd = cmd->next->next; //Past where to column
		temp = cmd;
		

		while(temp->tok_value != EOC && !rc && temp->tok_value != K_ORDER){
			int where_pointer = 0;
			// printf("%s",temp->tok_string);
			if(temp->tok_value == IDENT){
			// printf("%s",temp->tok_string);
				for(i = 0, temp_col = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
						i < tab_entry->num_columns ; i++, temp_col++){
					// printf("\n%s-%s\n",temp->tok_string, temp_col->col_name);
					if(strcmp(temp_col->col_name, temp->tok_string) == 0 ){
						col_found = true;
						// printf(temp->tok_string);
						if(num_where == 1){
							where_col_1 = temp;
							where_len[0] = temp_col->col_len;
						}else{
							where_col_2 = temp;
							where_len[1] = temp_col->col_len;
						}
						where_col_offset[where_col_i++] = where_pointer;
					}
					where_pointer += temp_col->col_len + 1;
				}

				if(!col_found && !rc){
					rc = INVALID_COLUMN_DEFINITION;
				}
				col_found = false;

			
			} else if(temp->tok_value == S_EQUAL || temp->tok_value == S_LESS || temp->tok_value == S_GREATER){
				where_ops[where_ops_i++] = temp->tok_value;
				// printf(temp->tok_string);
			} else if(temp->tok_value == K_AND && !or_flag){
				and_flag = true;
				num_where++;
				// printf(temp->tok_string);
			} else if(temp->tok_value == K_OR && !and_flag){
				or_flag = true;
				num_where++;
				// printf(temp->tok_string);
			} else if(temp->tok_value == 90 ) {
				// printf(temp->tok_string);
				if(num_where == 1){
					where_val_1 = temp;
				}else{
					where_val_2 = temp;
				}
			} else if( temp->tok_value == 91) {
				if(num_where == 1){
					where_val_1 = temp;
				}else{
					where_val_2 = temp;
				}
			} else {
				printf("\nInvalid Where Statement!\n");
				rc = INVALID_COLUMN_NAME;
			}
			// printf(temp->tok_string);
			temp = temp->next;
			// printf("\n");
		}
		//READ IN ORDER BY 
		// printf("%s\n", cmd->tok_string);
		// if(cmd->next->tok_value != EOC && cmd->next->tok_value == K_ORDER){
		// 	temp = cmd->next;
		// }

		if(temp->tok_value == K_ORDER){
			temp = temp->next->next;
			if(temp->tok_value == IDENT){
				order_flag = true;
				order_col = temp;
				printf("\nOrder by: %s ", order_col->tok_string);
				temp = temp->next;
				if(temp->tok_value == K_DESC){
					printf("%s", "DESC");
					order_desc = true;
				}
				printf("\n");
			}
			else {
				rc = INVALID_COLUMN_DEFINITION;
				printf("\nINVALID COLUMN\n");
			}

		}

		// printf("\n%d", num_where);
		// printf("\nCol1: %s Col2: %s",where_col_1->tok_string, where_col_2->tok_string);
		// printf("\nVal1: %s Val2: %s",where_val_1->tok_string, where_val_2->tok_string);
		// printf("\nTyp1: %d Typ2: %d",where_val_1->tok_value, where_val_2->tok_value);
		// printf("\nLen1: %d Len2: %d",where_len[0], where_len[1]);
		// printf("\nAnd: %d Or: %d", and_flag, or_flag);
		// printf("\nOps1: %d Ops2: %d", where_ops[0], where_ops[1]);
		// printf("\nColO1: %d ColO2: %d\n",where_col_offset[0], where_col_offset[1]);
		
		// printf(cmd->tok_string);
	} else if(cmd->next->tok_value != EOC && cmd->next->tok_value == K_ORDER){
		temp = cmd->next->next->next;

		if(temp->tok_value == IDENT){
				order_flag = true;
				order_col = temp;
				printf("\nOrder by: %s ", order_col->tok_string);
				temp = temp->next;
				if(temp->tok_value == K_DESC){
					printf("%s", "DESC");
					order_desc = true;
				}
				printf("\n");
			}
			else {
				rc = INVALID_COLUMN_DEFINITION;
				printf("\nINVALID COLUMN\n");
			}
	}
	// END WHERE FIND ______________________________________________________________________________
	printf("\n");
	// Reset cmd
	if(count_flag || avg_flag || sum_flag){
		cmd = t_list->next->next;
	} else {
		cmd = t_list;
	}
	
	// printf(cur->tok_string);
	if(!rc){
		if(cur->tok_value != K_FROM){
			printf("\nNO FROM ERROR\n");
			rc = TABLE_NOT_EXIST;
			cur->tok_value = INVALID;
		}else{
			cur = cur->next;//TABLE

			int num_tables = g_tpd_list->num_tables;
			// tab_entry = get_tpd_from_list(cur->tok_string);

			int len = strlen(tab_entry->table_name);
			char tab_name[MAX_IDENT_LEN+1];
			char *fname = new char[len+5];
			bool report = false;
			FILE *fhandle = NULL;
			struct _stat file_stat;

			if ((cur->tok_class != keyword) && (cur->tok_class != identifier) && (cur->tok_class != type_name)){
				// Error
				rc = INVALID_TABLE_NAME;
				cur->tok_value = INVALID;
			}
			else{

				strcpy(fname, tab_entry->table_name);
				fname[len] = '.';
				fname[len+1] = 't';
				fname[len+2] = 'a';
				fname[len+3] = 'b';
				fname[len+4] = '\0';

				// Open table file
			    if((fhandle = fopen(fname, "rbc")) == NULL){
					printf("File Error");
					rc = FILE_OPEN_ERROR;
				} else {
					// Get Memory
					_fstat(_fileno(fhandle), &file_stat);
					// printf("Table File Size: %d\n", file_stat.st_size);
					table = (table_file_header*)calloc(1, file_stat.st_size);

					if (!table){
						printf("Mem Error");
						rc = MEMORY_ERROR;
					} else {
						// Load File into Memory
						fread(table, file_stat.st_size, 1, fhandle);
						fflush(fhandle);
						fclose(fhandle);
						// printf("S:%d / N:%d / L:%d / O:%d\n\n", table->file_size, table->num_records, table->record_size, table->record_offset );
						
						if (table->file_size != file_stat.st_size){
							printf("Size Error\n");
							rc = DBFILE_CORRUPTION;
						} else {

							if(table->file_size == table->record_offset){
								printf("\nNo Records in Table\n");
								rc = -387;
							} else {

								// ORDER BY ________________________________________________________________________________
								if(order_flag){
									col_found = false;
									// int spec_pointer = 0;
									int col_off = 0;
									int col_len = 0;
									int col_typ = 0;

									int* pre_sort;
									int* post_sort;
									
									int col_local = 0;
									pre_sort = new int[table->num_records];
									post_sort = new int[table->num_records];

									for(int k = 0; k < table->num_records; k++){
										pre_sort[k] = 0;
										post_sort[k] = 0;
									}
									// printf("%s\n", order_col->tok_string);
									for(i = 0, temp_col = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
											i < tab_entry->num_columns; i++, temp_col++){		

										if(strcmp(temp_col->col_name, order_col->tok_string) == 0 ){
											col_found = true;

											col_len = temp_col->col_len;
											col_typ = temp_col->col_type;
										} 
										if(!col_found) {
											col_off += temp_col->col_len + 1;
										}
									}
									if(col_found){
										if(col_typ == T_INT){
											int pivot = 0;
											bool first = true;
											for(i = 0; i < table->num_records; i++){
												// printf("-%d-\n", i);
												int pointer = table->record_offset + col_off + 1;
												for(int entry_num = 0; entry_num < table->num_records; entry_num++){
													// int rlen = 0;
													// int *r = &rlen;

													// memcpy((void*)r, (void*)((char*)table + pointer++), sizeof(char));
													if(pre_sort[entry_num] == 0){
														int temp = 0;
														int *t = &temp;
														memcpy((void*)t, (void*)((char*)table + pointer), col_len);
														// printf(" T%d - P%d -L%d\n", temp, pivot, col_local);
														if(first){
															// printf("%s\n", "1");
															pivot = temp;
															col_local = entry_num;
															first = false;
														}
														if(order_desc){
															if( pivot > temp){
															// printf("%s\n", "2");

															pivot = temp;
															col_local = entry_num;
														}
														} else if(pivot < temp){
															// printf("%s\n", "3");
															pivot = temp;
															col_local = entry_num;
														}
														// printf(" T%d - P%d\n-------------------------------\n", temp, pivot);
													}

													pointer += table->record_size;
												}
												// printf("%s\n", "----------------------------------");
												first = true;
												post_sort[i] = col_local;
												pre_sort[col_local] = 1;
											}

											// for(i = 0; i < table->num_records; i++){
											// 	printf("%d ", pre_sort[i]);
											// }
											// printf("\n");
											// for(i = 0; i < table->num_records; i++){
											// 	printf("%d ", post_sort[i]);
											// }
											// printf("\n");

										} else {
																						bool first = true;
											for(i = 0; i < table->num_records; i++){
												// printf("-%d-\n", i);
												char *pivot = new char[col_len];
												pivot = "";
												int pointer = table->record_offset + col_off + 1;
												for(int entry_num = 0; entry_num < table->num_records; entry_num++){
													// int rlen = 0;
													// int *r = &rlen;

													// memcpy((void*)r, (void*)((char*)table + pointer++), sizeof(char));
													if(pre_sort[entry_num] == 0){
														char *ctemp = new char[col_len + 1];
														memcpy((void*)ctemp, (void*)((char*)table + pointer), col_len);
														ctemp[col_len] = '\0';
														// printf(" T-%s - P-%s -L-%d\n", ctemp, pivot, col_local);
														if(first){
															// printf("%s\n", "1");
															pivot = ctemp;
															col_local = entry_num;
															first = false;
														}
														if(order_desc){
															if(strcmp(pivot, ctemp) == -1){
															// printf("%s\n", "2");

															pivot = ctemp;
															col_local = entry_num;
														}
														} else if(strcmp(pivot, ctemp) == 1){
															// printf("%s\n", "3");
															pivot = ctemp;
															col_local = entry_num;
														}
														// printf(" T%d - P%d\n-------------------------------\n", temp, pivot);
													}

													pointer += table->record_size;
												}
												// printf("%s\n", "----------------------------------");
												first = true;
												post_sort[i] = col_local;
												pre_sort[col_local] = 1;
											}

											// for(i = 0; i < table->num_records; i++){
											// 	printf("%d ", pre_sort[i]);
											// }
											// printf("\n");
											// for(i = 0; i < table->num_records; i++){
											// 	printf("%d ", post_sort[i]);
											// }
											// printf("\n");
										}

										new_table = (table_file_header*)calloc(1, table->file_size);
										memset(new_table, 0, table->file_size);
										memcpy((void*)new_table, (void*)table, table->record_offset);

										int pointer = table->record_offset;
										int new_pointer = table->record_offset;
										for(int entry_num = 0; entry_num < table->num_records; entry_num++){
											pointer = table->record_offset + (table->record_size * post_sort[entry_num]);
											memcpy((void*)((char*)new_table + new_pointer), (void*)((char*)table + pointer), table->record_size);
											new_pointer += table->record_size;
										}
										table = new_table;
									}
								}
								// END ORDER BY ____________________________________________________________________________

								// SET VALID MATRIX ________________________________________________________________________
								col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);

								int* v_col;
								int* v_row;
								int* v_row2;

								v_col = new int[tab_entry->num_columns];
								v_row = new int[table->num_records];
								v_row2 = new int[table->num_records];

								for(int k = 0; k < tab_entry->num_columns; k++){
									v_col[k] = 0;
								}
								for(int k = 0; k < table->num_records; k++){
									v_row[k] = 0;
									v_row2[k] = 0;
								}
								// SET COLUMN VALID MATRIX ________________________________________________________________________
								// printf(cmd->tok_string);
								if(cmd->tok_value == S_STAR){

									for(int k = 0; k < tab_entry->num_columns; k++){
										v_col[k] = 1;
									}
									// for(int k = 0; k < table->num_records; k++){
									// 	v_row[k] = 1;
									// }

								} else if(cmd->tok_value == IDENT) {
									bool first = false;
									int num_to_select = 0;
									while(cmd->tok_value != K_FROM){
										// printf("%s\n",cmd->tok_string);
										
										for(i = 0, temp_col = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
											i < tab_entry->num_columns; i++, temp_col++){

											if(strcmp(temp_col->col_name, cmd->tok_string) == 0){
												v_col[i] = 1;
												first = true;
												num_to_select++;
											}
											if(!first){
												sel_col_offset += temp_col->col_len ;
											}
										}

										cmd = cmd->next;
										if(cmd->tok_value == S_COMMA){
											cmd = cmd->next;
										}
									}

									if(num_to_select == 0){
										printf("No Columns in Table\n\n");
										rc = INVALID_NO_TABLE_CONTENT;
									}
									cmd = t_list;

									// printf("Column Select\n");
								
								} else {
									printf("\nError\n");
									cmd->tok_value = INVALID;
									rc = INVALID;
								}
								// END COLUMN SET VALID MATRIX___________________________________________________________________________
								// SEW ROW VALID MATRIX__________________________________________________________________________________
								int num_row_selected = 0;

								if(where_flag){
									// printf("WHERE\n");
									if(where_val_1->tok_value == INT_LITERAL){
										int temp = 0;
										int *t = &temp;

										for(i = 0; i < table->num_records; i++){

											memcpy((void*)t,
											 (void*)((char*)table + table->record_offset + (table->record_size * i) + where_col_offset[0] + 1),
											  where_len[0]);
											// printf("%d\n",temp);
											if(where_ops[0] == S_EQUAL){
												if(atoi(where_val_1->tok_string) == temp ){
													v_row[i] = 1;
													num_row_selected++;
												}

											}else if(where_ops[0] == S_LESS){
												if(atoi(where_val_1->tok_string) > temp ){
													v_row[i] = 1;
													num_row_selected++;

												}
											} else {
												if(atoi(where_val_1->tok_string) < temp ){
													v_row[i] = 1;
													num_row_selected++;
												}
											}
										}
									} else {

										char *ctemp = new char[where_len[0] + 1]; 

										for(i = 0; i < table->num_records; i++){

											memcpy((void*)ctemp,
											 (void*)((char*)table + table->record_offset + (table->record_size * i) + where_col_offset[0] + 1),
											  where_len[0]);
											ctemp[where_len[0]] = '\0';
											// printf("%d\n",temp);
											if(where_ops[0] == S_EQUAL){
												if(strcmp(where_val_1->tok_string,ctemp) == 0){
													v_row[i] = 1;
													num_row_selected++;
												}
											}else if(where_ops[0] == S_LESS){
												if(strcmp(where_val_1->tok_string,ctemp) == -1){
													v_row[i] = 1;
													num_row_selected++;
												}
											} else {
												if(strcmp(where_val_1->tok_string,ctemp) == 1){
													v_row[i] = 1;
													num_row_selected++;
												}
											}
										}
									}
									// printf("%d\n",num_where);
									if(num_where == 2){
										num_row_selected = 0;
										if(where_val_2->tok_value == INT_LITERAL){
											int temp = 0;
											int *t = &temp;

											for(i = 0; i < table->num_records; i++){

												memcpy((void*)t,
												 (void*)((char*)table + table->record_offset + (table->record_size * i) + where_col_offset[1] + 1),
												  where_len[1]);
												// printf("%d\n",temp);
												if(where_ops[1] == S_EQUAL){
													if(atoi(where_val_2->tok_string) == temp ){
														v_row2[i] = 1;
													}

												}else if(where_ops[1] == S_LESS){
													if(atoi(where_val_2->tok_string) > temp ){
														v_row2[i] = 1;
													}
												} else {
													if(atoi(where_val_2->tok_string) < temp ){
														v_row2[i] = 1;
													}
												}
											}
										} else {

											char *ctemp = new char[where_len[1] + 1]; 

											for(i = 0; i < table->num_records; i++){

												memcpy((void*)ctemp,
												 (void*)((char*)table + table->record_offset + (table->record_size * i) + where_col_offset[01] + 1),
												  where_len[1]);
												ctemp[where_len[1]] = '\0';
												// printf("%d\n",temp);
												if(where_ops[1] == S_EQUAL){
													if(strcmp(where_val_2->tok_string,ctemp) == 0){
														v_row2[i] = 1;
													}
												}else if(where_ops[1] == S_LESS){
													if(strcmp(where_val_2->tok_string,ctemp) == -1){
														v_row2[i] = 1;
													}
												} else {
													if(strcmp(where_val_2->tok_string,ctemp) == 1){
														v_row2[i] = 1;
													}
												}
											}
										}
										// MERGE
										for(int k = 0; k < table->num_records; k++){
											if(and_flag){
												// v_row[k] = v_row[k] && v_row2[k];
												if(v_row[k] == 1 && v_row2[k] == 1){
													v_row[k] = 1;
													num_row_selected++;
												} else {
													v_row[k] = 0;
												}
											} else if(or_flag){
												// v_row[k] = v_row[k] || v_row2[k];
												if(v_row[k] == 1 || v_row2[k] == 1){
													v_row[k] = 1;
													num_row_selected++;
												} else {
													v_row[k] = 0;
												}
											} else {
												printf("ERROR in Operator\n");
											}
										}

									}
								}else{
									for(int k = 0; k < table->num_records; k++){
										v_row[k] = 1;
										num_row_selected++;
									}
								}
								// END SEW ROW VALID MATRIX______________________________________________________________________________
								// END VALID MATRIX______________________________________________________________________________________

								// SUM __________________________________________________________________________________________________
								if(sum_flag){
									col_sum = 0;
									col_found = false;
									// int spec_pointer = 0;
									int col_off = 0;
									int col_len = 0;
									int col_typ = 0;
									for(i = 0, temp_col = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
											i < tab_entry->num_columns; i++, temp_col++){
											
										if(strcmp(temp_col->col_name, special_col->tok_string) == 0 ){
											col_found = true;
											col_len = temp_col->col_len;
											col_typ = temp_col->col_type;
										}
										if(!col_found){
											col_off += temp_col->col_len + 1;
										}
										
									}
									if(col_found){
										if(col_typ == T_INT){
											int pointer = table->record_offset + col_off + 1;
											for(int entry_num = 0; entry_num < table->num_records; entry_num++){
												if(v_row[entry_num] == 1){
													// int rlen = 0;
													// int *r = &rlen;

													// memcpy((void*)r, (void*)((char*)table + pointer++), sizeof(char));


													int temp = 0;
													int *t = &temp;
													memcpy((void*)t, (void*)((char*)table + pointer), col_off);
													// printf(" %d -\n", temp);
													col_sum += temp;
												}
												pointer += table->record_size;

											}
										} else {
											printf("\nCant SUM CHAR COLUMN\n");
											rc = INVALID_COLUMN_DEFINITION;
											fail = true;
										}
									}
								}
								// END SUM ______________________________________________________________________________________________
								// AVG __________________________________________________________________________________________________
								if(avg_flag){
									col_avg = 0;
									col_sum = 0;
									col_found = false;
									// int spec_pointer = 0;
									int col_off = 0;
									int col_len = 0;
									int col_typ = 0;
									for(i = 0, temp_col = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
											i < tab_entry->num_columns; i++, temp_col++){
											
										if(strcmp(temp_col->col_name, special_col->tok_string) == 0 ){
											col_found = true;
											col_len = temp_col->col_len;
											col_typ = temp_col->col_type;
										}if(!col_found){
											col_off += temp_col-> col_len + 1;
										}
									}
									if(col_found){
										if(col_typ == T_INT){
											int pointer = table->record_offset + col_off + 1;
											for(int entry_num = 0; entry_num < table->num_records; entry_num++){
												// int rlen = 0;
												// int *r = &rlen;

												// memcpy((void*)r, (void*)((char*)table + pointer++), sizeof(char));

												if(v_row[entry_num] == 1){
													int temp = 0;
													int *t = &temp;
													memcpy((void*)t, (void*)((char*)table + pointer), col_off);
													// printf(" %d -\n", temp);
													col_sum += temp;
												}
												pointer += table->record_size;
											}
											col_avg = col_sum/num_row_selected;
										} else {
											printf("\nCant AVG CHAR COLUMN\n");
											rc = INVALID_COLUMN_DEFINITION;
											fail = true;
										}
									}
								}
								// END AVG ______________________________________________________________________________________________

								// PRINT TABLE __________________________________________________________________________________________
								if(!rc && num_row_selected != 0 && !count_flag && !sum_flag && !avg_flag){

									// printf("\nSELECT %s FROM %s\n\n",cmd->tok_string, cur->tok_string);
									// printf("TS: %d\t", table->file_size);
									// printf("RS: %d\n", table->record_size);

									printf("-----------------------\n");
									printf("| %-19s |\n-", cur->tok_string);
									for(j = 0; j < tab_entry->num_columns;j++){
										if(v_col[j] == 1){printf("----------------------");}
									}
									// printf("-----------------------------------------------------------------------------------------\n");
									printf("\n|");

									for(i = 0, temp_col = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
										i < tab_entry->num_columns; i++, temp_col++){
										if(v_col[i] == 1){printf(" %-19s |", temp_col->col_name);}
									}
									printf("\n-");

									// printf("-----------------------------------------------------------------------------------------\n");
									for(j = 0; j < tab_entry->num_columns;j++){
										if(v_col[j] == 1){printf("----------------------");}
									}
									printf("\n");

									for(int entry_num = 0; entry_num < table->num_records; entry_num++){
										int pointer = table->record_offset + table->record_size * entry_num + sel_col_offset;

										if(v_row[entry_num] == 1){
											printf("|");
											for(i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
												i < tab_entry->num_columns; i++, col_entry++){

												if(col_entry->col_type == T_INT){
													// Int
												
													int rlen = 0;
													int *r = &rlen;

													// printf("Col_Name: %s\t",col_entry->col_name);
													// printf("Col_Type: %d\t",col_entry->col_type);

													memcpy((void*)r, (void*)((char*)table + pointer++), sizeof(char));
													// printf("Size: %d\t", rlen);
													if(v_col[i] == 1){
														int temp = 0;
														int *t = &temp;

														memcpy((void*)t, (void*)((char*)table + pointer), rlen);
														printf(" %19d |", temp);
													}

													pointer += rlen;
													// col_entry++;
												} else {
													// Char
													int rlen = 0;
													int *r = &rlen;

													// printf("Col_Name: %s\t",col_entry->col_name);
													// printf("Col_Type: %d\t",col_entry->col_type);

													memcpy((void*)r, (void*)((char*)table + pointer++), sizeof(char));
													// printf("Size: %d\t", rlen);
													if(v_col[i] == 1){
														char *ctemp = new char[rlen + 1]; 
														// char *c = &ctemp;

														memcpy((void*)ctemp, (void*)((char*)table + pointer), rlen);
														ctemp[rlen] = '\0';
														printf(" %-19s |", ctemp);
													}	

													pointer += rlen;
												}
												
											}
											printf("\n");
										}
									}
									for(j = 0; j < tab_entry->num_columns;j++){
										if(v_col[j] == 1){
											printf("----------------------");
										}
									}
									printf("-\n");
								}
								if(count_flag && !fail){
									printf("-----------------------\n");
									printf("| %-19s |\n-", "COUNT");
									printf("----------------------\n");
									printf("| %19d |\n-", num_row_selected);
									printf("----------------------\n");

								} else if(avg_flag && !fail){
									printf("-----------------------\n");
									printf("| %-19s |\n-", "AVG");
									printf("----------------------\n");
									printf("| %19f |\n-", col_avg);
									printf("----------------------\n");

								} else if(sum_flag && !fail){
									printf("-----------------------\n");
									printf("| %-19s |\n-", "SUM");
									printf("----------------------\n");
									printf("| %19f |\n-", col_sum);
									printf("----------------------\n");

								} else if(!fail){
									printf(" %d rows selected.\n", num_row_selected);
								}
							}
							// END PRINT TABLE ______________________________________________________________________________________
						}
					}
				}
			}
		}
	}
	return rc;
}

int sem_delete_stmt(token_list *t_list){
	int rc = 0;
	int i = 0;

	token_list *cur;
	tpd_entry *tab_entry = NULL;
	cd_entry  *col_entry = NULL;
	table_file_header *table = NULL;
	table_file_header *new_table = NULL;
	table_file_header *old_table = NULL;
	table_file_header *new_entry = NULL;

	int num_tables = g_tpd_list->num_tables;
	tab_entry = get_tpd_from_list(t_list->tok_string);

	if(tab_entry){

		cur = t_list;

		int len = strlen(tab_entry->table_name);
		char tab_name[MAX_IDENT_LEN+1];
		char *fname = new char[len+5];
		bool report = false;
		FILE *fhandle = NULL;
		struct _stat file_stat;
		

		if ((cur->tok_class != keyword) && (cur->tok_class != identifier) && (cur->tok_class != type_name)){
			// Error
			rc = INVALID_TABLE_NAME;
			cur->tok_value = INVALID;
		} else {

			strcpy(fname, tab_entry->table_name);
			fname[len] = '.';
			fname[len+1] = 't';
			fname[len+2] = 'a';
			fname[len+3] = 'b';
			fname[len+4] = '\0';

			// Open table file
		    if((fhandle = fopen(fname, "rbc")) == NULL)
			{
				printf("File Error");
				rc = FILE_OPEN_ERROR;
			} else {
				// Get Memory
				_fstat(_fileno(fhandle), &file_stat);
				// printf("Table File Size: %d\n", file_stat.st_size);
				table = (table_file_header*)calloc(1, file_stat.st_size);

				if (!table)
				{
					printf("Mem Error");
					rc = MEMORY_ERROR;
				} else {
				 
					// Load File into Memory
					fread(table, file_stat.st_size, 1, fhandle);
					fflush(fhandle);
					fclose(fhandle);
					printf("S:%d / N:%d / L:%d / O:%d\n\n", table->file_size, table->num_records, table->record_size, table->record_offset );
					
					if (table->file_size != file_stat.st_size){
						printf("Size Error\n");
						rc = DBFILE_CORRUPTION;
					} else {
						if(table->file_size == table->record_offset){
							printf("\n No Records in Table to Delete\n");
							rc = -387;
						} else {
							cur = cur->next;
							if (cur->tok_value != K_WHERE){
								rc = INVALID_TABLE_DEFINITION;
								cur->tok_value = INVALID;
							} else {
								cur = cur->next;
								char *column = new char[strlen(cur->tok_string)];
								strcpy(column, cur->tok_string);
								// printf("Column: %s\n",column);

								cur = cur->next;
								// char *ops = new char[strlen(cur->tok_string)];
								// strcpy(ops, cur->tok_string);
								int ops;
								ops = cur->tok_value;
								// printf("Ops: %d\n",ops);

								cur = cur->next;
								char *val = new char[strlen(cur->tok_string)];
								strcpy(val, cur->tok_string);
								// printf("Value: %s\n",val);

								col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);

								int rec_offset = 0;
								int tot_offset = 0;
								int col_len = 0;
								int col_typ = 0;
								int total = 0;

								for(int i = 0; i < tab_entry->num_columns; i++){
									
									if (strcmp(col_entry->col_name, column)== 0){
										rec_offset = ++total;
										tot_offset = table->record_offset + total;
										col_len = col_entry->col_len;
										col_typ = col_entry->col_type;
										// printf("Rec Offset %d\n", rec_offset);
										// printf("Tot Offset %d\n", tot_offset);
										// printf("Rec Length %d\n", col_len);
									} else {
										total += col_entry->col_len + 1;
									}
									col_entry++;
								}

								int pointer = tot_offset;
								int row_start = table->record_offset + 1;
								int rlen = col_len;

								int num_del = 0;
								bool del = false;


								old_table = (table_file_header*)calloc(1,  table->record_offset);		
								memset(old_table, 0, table->record_offset);
								memcpy((void*)old_table, (void*)table, table->record_offset);
								old_table->file_size = table->record_offset;
								old_table->num_records = 0;

								for(int i = 0; i < table->num_records; i++){

									if(col_typ == T_INT){
										int temp = 0;
										int *t = &temp;

										memcpy((void*)t, (void*)((char*)table + pointer), rlen);
										// printf(" %d -", row_start);
										// printf(" %d ", temp);

										if(ops == 74){
											if(atoi(val) == temp){
												num_del++;
												// printf(" -DEL\n");
												del = true;
											} else {
												// printf("\n");
											}
										} else if(ops == 75){
											if(atoi(val) > temp){
												num_del++;
												// printf(" -DEL\n");
												del = true;
											} else {
												// printf("\n");
											}
										} else {
											if(atoi(val) < temp){
												num_del++;
												// printf(" -DEL\n");
												del = true;
											} else {
												// printf("\n");
											}
										}

									} else {
										char *ctemp = new char[rlen + 1]; 

										memcpy((void*)ctemp, (void*)((char*)table + pointer), rlen);
										ctemp[rlen] = '\0';
										// printf(" %d -", row_start);
										// printf(" %s ", ctemp);


										if(ops == 74){
											if(strcmp(val,ctemp) == 0){
												num_del++;
												// printf(" -DEL\n");
												del = true;
											} else {
												// printf("\n");
											}
										} else if(ops == 75){
											if(strcmp(val,ctemp) == 1){
												num_del++;
												// printf(" -DEL\n");
												del = true;
											} else {
												// printf("\n");
											}
										} else {
											if(strcmp(val,ctemp) == -1){
												num_del++;
												// printf(" -DEL\n");
												del = true;
											} else {
												// printf("\n");
											}
										}

									}

									if(!del){
										// free(new_table);
										new_table = (table_file_header*)calloc(1, old_table->file_size + table->record_size);		
										memset(new_table, 0, old_table->file_size + table->record_size);
										memcpy((void*)new_table, (void*)old_table, old_table->file_size);
										memcpy((void*)((char*)new_table + old_table->file_size), (void*)((char*)table + row_start - 1), table->record_size);
										new_table->file_size = old_table->file_size + table->record_size;
										new_table->num_records += 1;
										// free(old_table);
										old_table = new_table;
									}

									del = false;
									pointer+= table->record_size;
									row_start += table->record_size;
								}

								if(num_del==table->num_records){
									new_table = (table_file_header*)calloc(1, table->record_offset);
									memset(new_table, 0, table->record_offset);
									memcpy((void*)new_table, (void*)table, table->record_offset);
									new_table->num_records = 0;
									new_table->file_size = table->record_offset;
								}

								if(num_del > 0){
									printf("Number of Rows Deleted: %d\n", num_del);
									// SWITCH TO fname
									rc = write_to_table_file(new_table, fname);
									free(new_table);
								} else {
									printf("\nNo Rows Deleted!\n");
								}
							}
						}
					}
				}
			}
		}
	} else {
		printf("\nTable Does not Exists\n");
		rc = INVALID_TABLE_DEFINITION;
	}
	return rc;
}

int sem_update_stmt(token_list *t_list){
	int rc = 0;
	int i = 0;

	token_list *cur;
	tpd_entry *tab_entry = NULL;
	cd_entry  *col_entry = NULL;
	table_file_header *table = NULL;
	// table_file_header *new_table = NULL;
	// table_file_header *old_table = NULL;
	table_file_header *new_entry = NULL;

	int num_tables = g_tpd_list->num_tables;
	tab_entry = get_tpd_from_list(t_list->tok_string);

	if(tab_entry){


		cur = t_list;

		int len = strlen(tab_entry->table_name);
		char tab_name[MAX_IDENT_LEN+1];
		char *fname = new char[len+5];
		bool report = false;
		FILE *fhandle = NULL;
		struct _stat file_stat;
		

		if ((cur->tok_class != keyword) && (cur->tok_class != identifier) && (cur->tok_class != type_name)){
			// Error
			rc = INVALID_TABLE_NAME;
			cur->tok_value = INVALID;
		} else {

			strcpy(fname, tab_entry->table_name);
			fname[len] = '.';
			fname[len+1] = 't';
			fname[len+2] = 'a';
			fname[len+3] = 'b';
			fname[len+4] = '\0';

			// Open table file
		    if((fhandle = fopen(fname, "rbc")) == NULL)
			{
				printf("File Error");
				rc = FILE_OPEN_ERROR;
			} else {
				// Get Memory
				_fstat(_fileno(fhandle), &file_stat);
				// printf("Table File Size: %d\n", file_stat.st_size);
				table = (table_file_header*)calloc(1, file_stat.st_size);

				if (!table)
				{
					printf("Mem Error");
					rc = MEMORY_ERROR;
				} else {
				 
					// Load File into Memory
					fread(table, file_stat.st_size, 1, fhandle);
					fflush(fhandle);
					fclose(fhandle);
					// printf("S:%d / N:%d / L:%d / O:%d\n\n", table->file_size, table->num_records, table->record_size, table->record_offset );
					
					if (table->file_size != file_stat.st_size){
						printf("Size Error\n");
						rc = DBFILE_CORRUPTION;
					} else {
						cur = cur->next;
						if (cur->tok_value != K_SET){
							rc = INVALID_TABLE_DEFINITION;
							cur->tok_value = INVALID;
						} else {

							cur = cur->next;
							char *upd_column = new char[strlen(cur->tok_string)];
							strcpy(upd_column, cur->tok_string);
							// printf("Up-Column: %s\n",upd_column);

							cur = cur->next;
							// =
							cur = cur->next;
							char *upd_value = new char[strlen(cur->tok_string)];
							strcpy(upd_value, cur->tok_string);
							// printf("Up-Value: %s\n",upd_value);

							int upd_type;
							upd_type = cur->tok_value;

							cur = cur->next;
							// = Where

							cur = cur->next;
							char *column = new char[strlen(cur->tok_string)];
							strcpy(column, cur->tok_string);
							// printf("Column: %s\n",column);

							cur = cur->next;
							int ops;
							ops = cur->tok_value;
							// printf("Ops: %d\n",ops);

							cur = cur->next;
							char *val = new char[strlen(cur->tok_string)];
							strcpy(val, cur->tok_string);
							// printf("Value: %s\n",val);

							col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);

							int rec_offset = 0;
							int tot_offset = 0;
							int col_len = 0;
							int col_typ = 0;
							int total = 0;

							for(int i = 0; i < tab_entry->num_columns; i++){
								
								if (strcmp(col_entry->col_name, column)== 0){
									rec_offset = ++total;
									tot_offset = table->record_offset + total;
									col_len = col_entry->col_len;
									col_typ = col_entry->col_type;
									// printf("Rec Offset %d\n", rec_offset);
									// printf("Tot Offset %d\n", tot_offset);
									// printf("Rec Length %d\n", col_len);
								} else {
									total += col_entry->col_len + 1;
								}
								col_entry++;
							}

							col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);

							int upd_offset = 0;
							int upd_len = 0;
							int upd_typ = 0;
							total = 0;
							
							for(int i = 0; i < tab_entry->num_columns; i++){
								
								if (strcmp(col_entry->col_name, upd_column)== 0){
									upd_offset = ++total;
									upd_len = col_entry->col_len;
									upd_typ = col_entry->col_type;
									// printf("Upd Offset %d\n", upd_offset);
									// printf("Upd Length %d\n", upd_len);
									// printf("Upd Type %d\n", upd_typ);
								} else {
									total += col_entry->col_len + 1;
								}
								col_entry++;
							}

							if((upd_typ == 10 && upd_type == 90) || (upd_typ == 11 && upd_type == 91)){

								int pointer = tot_offset;
								int row_start = table->record_offset + 1;
								int rlen = col_len;

								int num_upd = 0;
								bool upd = false;


		// 						old_table = (table_file_header*)calloc(1,  table->file_size);		
		// 						memset(old_table, 0, table->file_size);
		// 						memcpy((void*)old_table, (void*)table, table->file_size);
		// ;

								for(int i = 0; i < table->num_records; i++){

									if(col_typ == T_INT){
										int temp = 0;
										int *t = &temp;

										memcpy((void*)t, (void*)((char*)table + pointer), rlen);
										// printf(" %d -", row_start);
										// printf(" %d ", temp);

										if(ops == 74){
											if(atoi(val) == temp){
												num_upd++;
												// printf(" -Up\n");
												upd = true;
											} else {
												// printf("\n");
											}
										} else if(ops == 75){
											if(atoi(val) > temp){
												num_upd++;
												// printf(" -Up\n");
												upd = true;
											} else {
												// printf("\n");
											}
										} else {
											if(atoi(val) < temp){
												num_upd++;
												// printf(" -Up\n");
												upd = true;
											} else {
												// printf("\n");
											}
										}

									} else {
										char *ctemp = new char[rlen + 1]; 

										memcpy((void*)ctemp, (void*)((char*)table + pointer), rlen);
										ctemp[rlen] = '\0';
										// printf(" %d -", row_start);
										// printf(" %s ", ctemp);


										if(ops == 74){
											if(strcmp(val,ctemp) == 0){
												num_upd++;
												// printf(" -Up\n");
												upd = true;
											} else {
												// printf("\n");
											}
										} else if(ops == 75){
											if(strcmp(val,ctemp) == 1){
												num_upd++;
												// printf(" -Up\n");
												upd = true;
											} else {
												// printf("\n");
											}
										} else {
											if(strcmp(val,ctemp) == -1){
												num_upd++;
												// printf(" -Up\n");
												upd = true;
											} else {
												// printf("\n");
											}
										}

									}

									if(upd){
										if((upd_typ == T_INT)){
											int temp = atoi(upd_value);
											int *t = &temp;
											memcpy((void*)((char*)table + row_start + upd_offset - 1), (void*)t, sizeof(int));
										} else {
											memcpy((void*)((char*)table + row_start + upd_offset - 1), (void*)upd_value, upd_len);
										}
									}

									upd = false;
									pointer+= table->record_size;
									row_start += table->record_size;
								}

								if(num_upd > 0){
									printf("\nNumber of Rows Updated: %d\n", num_upd);
									// SWITCH TO fname
									rc = write_to_table_file(table, fname);
									// free(new_table);
								} else {
									printf("\nNo Rows Updated!\n");
								}
							} else {
								printf("\nMissMatch Error\n");
								rc = -390;
							}
						}
					}
				}
			}
		}
	} else {
		printf("\nTable Does not Exists\n");
		rc = INVALID_TABLE_DEFINITION;
	}
	return rc;
}

int write_to_table_file(table_file_header *ntable, char *tablename){
	int rc = 0;
	FILE *fhandle = NULL;
	struct _stat file_stat;
	table_file_header *tfh;

	tfh = NULL;
	tfh = ntable;

	size_t len = strlen(tablename);
	char *fname = new char[len];
	strcpy(fname, tablename);

	// printf("Table File: %s\t", fname);
	// printf("File Size: %d\n", tfh->file_size);
	
	if((fhandle = fopen(fname, "wbc")) == NULL)
	{
		rc = FILE_OPEN_ERROR;
	}
	else
	{
		// tfh = (table_file_header*)calloc(1, sizeof(table_file_header));
		// memset((void*)tfh, '\0', sizeof(table_file_header));
		
		if (!tfh)
		{
			rc = MEMORY_ERROR;
		}
		else
		{
			fwrite(tfh, tfh->file_size, 1, fhandle);
			fflush(fhandle);
			fclose(fhandle);
		}
	}

	return rc;
}

int initialize_table_list(char *tablename, int rec_size){
	int rc = 0;
	FILE *fhandle = NULL;
	struct _stat file_stat;
	table_file_header *tfh;

	size_t len = strlen(tablename);
	char *fname = new char[len+5];
	strcpy(fname, tablename);
	fname[len] = '.';
	fname[len+1] = 't';
	fname[len+2] = 'a';
	fname[len+3] = 'b';
	fname[len+4] = '\0';
	printf("Table File: %s\n", fname);
	printf("Rec Size: %d\n", rec_size);
	
	if((fhandle = fopen(fname, "wbc")) == NULL)
	{
		rc = FILE_OPEN_ERROR;
	}
	else
	{
		tfh = NULL;
		tfh = (table_file_header*)calloc(1, sizeof(table_file_header));
		memset((void*)tfh, '\0', sizeof(table_file_header));
		
		if (!tfh)
		{
			rc = MEMORY_ERROR;
		}
		else
		{
			tfh->record_size = rec_size;
			tfh->record_offset = sizeof(table_file_header);
			tfh->file_size = sizeof(table_file_header);
			fwrite(tfh, sizeof(table_file_header), 1, fhandle);
			fflush(fhandle);
			fclose(fhandle);
		}
	}

	return rc;
}

int delete_table_file(char * tablename) {
	int rc = 0;

	size_t len = strlen(tablename);
	char *fname = new char[len+5];
	strcpy(fname, tablename);
	fname[len] = '.';
	fname[len+1] = 't';
	fname[len+2] = 'a';
	fname[len+3] = 'b';
	fname[len+4] = '\0';
	printf("Deleting Table File: %s\n", fname);

	if( remove(fname) != 0 ){
	    // printf( "Error deleting file" );
		rc = FILE_DELETE_ERROR;
	}
  	// else{
	  //   printf( "File successfully deleted" );
  	// }
  	return rc;
}

int initialize_tpd_list()
{
	int rc = 0;
	FILE *fhandle = NULL;
	struct _stat file_stat;

    /* Open for read */
    if((fhandle = fopen("dbfile.bin", "rbc")) == NULL)
	{
		if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
		{
			rc = FILE_OPEN_ERROR;
		}
    	else
		{
			g_tpd_list = NULL;
			g_tpd_list = (tpd_list*)calloc(1, sizeof(tpd_list));
			
			if (!g_tpd_list)
			{
				rc = MEMORY_ERROR;
			}
			else
			{
				g_tpd_list->list_size = sizeof(tpd_list);
				fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
				fflush(fhandle);
				fclose(fhandle);
			}
		}
	}
	else
	{
		/* There is a valid dbfile.bin file - get file size */
		_fstat(_fileno(fhandle), &file_stat);
		printf("dbfile.bin size = %d\n", file_stat.st_size);

		g_tpd_list = (tpd_list*)calloc(1, file_stat.st_size);

		if (!g_tpd_list)
		{
			rc = MEMORY_ERROR;
		}
		else
		{
			fread(g_tpd_list, file_stat.st_size, 1, fhandle);
			fflush(fhandle);
			fclose(fhandle);

			if (g_tpd_list->list_size != file_stat.st_size)
			{
				rc = DBFILE_CORRUPTION;
			}

		}
	}
    
	return rc;
}
	
int add_tpd_to_list(tpd_entry *tpd)
{
	int rc = 0;
	int old_size = 0;
	FILE *fhandle = NULL;

	if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
	{
		rc = FILE_OPEN_ERROR;
	}
  	else
	{
		old_size = g_tpd_list->list_size;

		if (g_tpd_list->num_tables == 0)
		{
			/* If this is an empty list, overlap the dummy header */
			g_tpd_list->num_tables++;
		 	g_tpd_list->list_size += (tpd->tpd_size - sizeof(tpd_entry));
			fwrite(g_tpd_list, old_size - sizeof(tpd_entry), 1, fhandle);
		}
		else
		{
			/* There is at least 1, just append at the end */
			g_tpd_list->num_tables++;
		 	g_tpd_list->list_size += tpd->tpd_size;
			fwrite(g_tpd_list, old_size, 1, fhandle);
		}

		fwrite(tpd, tpd->tpd_size, 1, fhandle);
		fflush(fhandle);
		fclose(fhandle);
	}

	return rc;
}

int drop_tpd_from_list(char *tabname)
{
	int rc = 0;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;
	int count = 0;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (stricmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				int old_size = 0;
				FILE *fhandle = NULL;

				if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
				{
					rc = FILE_OPEN_ERROR;
				}
			  else
				{
					old_size = g_tpd_list->list_size;

					if (count == 0)
					{
						/* If this is the first entry */
						g_tpd_list->num_tables--;

						if (g_tpd_list->num_tables == 0)
						{
							/* This is the last table, null out dummy header */
							memset((void*)g_tpd_list, '\0', sizeof(tpd_list));
							g_tpd_list->list_size = sizeof(tpd_list);
							fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
						}
						else
						{
							/* First in list, but not the last one */
							g_tpd_list->list_size -= cur->tpd_size;

							/* First, write the 8 byte header */
							fwrite(g_tpd_list, sizeof(tpd_list) - sizeof(tpd_entry),
								     1, fhandle);

							/* Now write everything starting after the cur entry */
							fwrite((char*)cur + cur->tpd_size,
								     old_size - cur->tpd_size -
										 (sizeof(tpd_list) - sizeof(tpd_entry)),
								     1, fhandle);
						}
					}
					else
					{
						/* This is NOT the first entry - count > 0 */
						g_tpd_list->num_tables--;
					 	g_tpd_list->list_size -= cur->tpd_size;

						/* First, write everything from beginning to cur */
						fwrite(g_tpd_list, ((char*)cur - (char*)g_tpd_list),
									 1, fhandle);

						/* Check if cur is the last entry. Note that g_tdp_list->list_size
						   has already subtracted the cur->tpd_size, therefore it will
						   point to the start of cur if cur was the last entry */
						if ((char*)g_tpd_list + g_tpd_list->list_size == (char*)cur)
						{
							/* If true, nothing else to write */
						}
						else
						{
							/* NOT the last entry, copy everything from the beginning of the
							   next entry which is (cur + cur->tpd_size) and the remaining size */
							fwrite((char*)cur + cur->tpd_size, old_size - cur->tpd_size -
										 ((char*)cur - (char*)g_tpd_list), 1, fhandle);
						}
					}

					fflush(fhandle);
					fclose(fhandle);
				}

			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
					count++;
				}
			}
		}
	}
	
	if (!found)
	{
		rc = INVALID_TABLE_NAME;
	}

	return rc;
}

tpd_entry* get_tpd_from_list(char *tabname)
{
	tpd_entry *tpd = NULL;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (stricmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				tpd = cur;
			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
				}
			}
		}
	}

	return tpd;
}