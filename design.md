 ## Design

In order to be able to extract information using a LLM, we need to go through the following steps

1. Read the <accession_number>-index-headers.html file, in order to get a list of files in the filing.
2. From the index headers, find the main filing file.
3. If the main filing file is a html, use html2text to convert to text. preserve the links in the file. The tables will be converted to markdown tables. Below is the setting for html2text
   ```py
        converter = html2text.HTML2Text()
        converter.ignore_links = False
        converter.ignore_images = True
        converter.ignore_emphasis = True
        converter.body_width = 0
    ```

4. split the text (from html file) into multiple chunks, approx. 3500 characters in size. Try to keep the paragraphs and markdown table in the same chunk if possible, in order to preserve the context.

5. Store the text chunks into a BigQuery table with the following schema
   | field            | descrption                                         |
   | ---------------- | -------------------------------------------------- |
   | accession_number | uniquely identify a filing                         |
   | seq              | seq number of the chunk                            |
   | content          | text of the chunk                                  |
   | model            | mode used to create the embedding. initially empty |
   | embedding        | ARRAY<FLOAT64>, embedding. initially empty         |

6. Run ML.GENERATE_EMBEDDING in BigQuery to get text embedding and populate embedding/model fields.
7. export embedding to a json file, tag with accession_number and seq (hopefully content as well), load into a Vertex AI Vector Search database
