package com.lgi.indexer;

import org.apache.lucene.analysis.standard.StandardAnalyzer;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.List;
import java.util.stream.Collectors;
import java.util.AbstractMap;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.util.Base64;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

public class RedisToLuceneIndexer {

	private static final Logger logger =
			Logger.getLogger(RedisToLuceneIndexer.class.getName());

	private static final String REDIS_HOST = "localhost";
	private static final int REDIS_PORT = 6379;
	private static final String ID_TO_DOC_MAP = "id_to_doc_map";
	private static final String ID_TO_REFERER_MAP = "id_to_referer_map";
	private static final String ID_TO_REFERRED_MAP = "id_to_referred_map";

	private static final String LUCENE_INDEX_PATH = "lucene_index";

	private static Directory luceneIndexDirectory;
	private static StandardAnalyzer analyzer;
	private static IndexWriterConfig config;
	private static IndexWriter indexWriter;
	private static Jedis jedis;

	static {
		try {
			luceneIndexDirectory = FSDirectory.open(FileSystems.getDefault().getPath(LUCENE_INDEX_PATH));
			analyzer = new StandardAnalyzer();
			config = new IndexWriterConfig(analyzer);
			indexWriter = new IndexWriter(luceneIndexDirectory, config);
			jedis = new Jedis(REDIS_HOST, REDIS_PORT);
			LogManager.getLogManager()
			.readConfiguration(RedisToLuceneIndexer.class.getResourceAsStream("/logging.properties"));
		} catch (IOException e) {
			e.printStackTrace(); // Handle the exception based on your needs
		}
	}

	private static ScanResult<Map.Entry<String, String>> getHashEntries(Jedis jedis, 
			String hashKey, 
			String cursor,
			int page_size) {
		ScanParams scanParams = new ScanParams().count(page_size);
		return jedis.hscan(hashKey, cursor, scanParams);
	}

	private static Set<Integer> getNonExistingKeysInSortedSet(Jedis jedis,
			String sortedSetKey, Set<Integer> keys) {
		return keys.stream()
				.filter(key -> !keyExistsInSortedSet(jedis, sortedSetKey, key))
				.collect(Collectors.toSet());
	}

	private static boolean keyExistsInSortedSet(Jedis jedis, String sortedSetKey, int member) {
		Double score = jedis.zscore(sortedSetKey, String.valueOf(member));
		return score != null;
	}

	private static void storeKeysInTempSortedSet(Set<Integer> entries,
			String tempSortedSet) {
		entries.stream()
		.map(entry -> new AbstractMap.SimpleEntry<>(entry.doubleValue(), String.valueOf(entry)))
		.forEach(pair -> jedis.zadd(tempSortedSet, pair.getKey(), pair.getValue()));
	}

	private static void readDataFromRedisAndIndex(Set<Integer> keys,
			IndexWriter indexWriter) throws IOException {
		for (Integer key : keys) {
			String caseValue = jedis.hget(ID_TO_DOC_MAP, String.valueOf(key));

			// logger.info("This is an info message." + field + " " + caseValue);
			// Index the data into Lucene
			indexDataIntoLucene(indexWriter, caseValue);
			// Process each key as needed
			System.out.println("Processing key: " + key);
		}
	}

	private static byte[] decompress(byte[] compressedData) throws DataFormatException {
		Inflater inflater = new Inflater();
		inflater.setInput(compressedData);

		// Create a ByteArrayOutputStream to hold the decompressed data
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

		byte[] buffer = new byte[1024];
		int length;

		try {
			while (!inflater.finished()) {
				length = inflater.inflate(buffer);
				outputStream.write(buffer, 0, length);
			}
		} finally {
			inflater.end();
		}

		return outputStream.toByteArray();
	}

	private static void indexDataIntoLucene(IndexWriter indexWriter, String casevalue) throws IOException {
		try {
			// Parse the JSON string using Jackson
			ObjectMapper objectMapper = new ObjectMapper();
			JsonNode jsonNode = objectMapper.readTree(casevalue);

			// Create Lucene document
			Document luceneDocument = new Document();

			// Iterate over JSON keys and add them to Lucene document
			JsonNode valuesNode = jsonNode.path("_values");
			valuesNode.fields().forEachRemaining(entry -> {
				String key = entry.getKey();
				String value = entry.getValue().asText();
				if (key.equals("case_judgement")) {
					byte[] decodedBytes = Base64.getDecoder().decode(value);

					// Decompress using Inflater
					try {
						byte[] decompressedBytes = decompress(decodedBytes);

						// Convert the decompressed bytes back to text
						String decompressedText = new String(decompressedBytes, "UTF-8");

						// Use the decompressed text as needed
						luceneDocument.add(new Field(key, decompressedText, TextField.TYPE_NOT_STORED));
					} catch (DataFormatException e) {
						e.printStackTrace();
					} catch (Exception e) {
						e.printStackTrace();
					}
				} else {
					luceneDocument.add(new StringField(key, value, Field.Store.YES));
				}
			});

			// Use the Lucene document as needed
			//System.out.println("Lucene Document: " + luceneDocument);
			indexWriter.addDocument(luceneDocument);
		} catch (IOException e) {
			e.printStackTrace();
		}  
	}

	void readFromStaticRedis()
	{
		try {
			String tempSortedSet = "processed_key_set";
			int pageSize = 1000; // Adjust the page size based on your needs


			// Start the pagination loop
			String cursor = "0";
			ScanResult<Map.Entry<String, String>> scanResult;

			do {
				// Get a page of keys from the hash
				scanResult =
						getHashEntries(jedis, ID_TO_DOC_MAP, cursor, pageSize);

				// Convert keys to integers
				Set<Integer> keysAsIntegers = scanResult.getResult().stream()
						.map(entry -> Integer.parseInt(entry.getKey()))
						.collect(Collectors.toSet());

				// Find the difference between tempSortedSet and the current page of keys
				Set<Integer> newKeys = getNonExistingKeysInSortedSet(jedis, tempSortedSet, keysAsIntegers);

				// Process only the new keys
				readDataFromRedisAndIndex(newKeys, indexWriter);

				// Store the current page of keys in the temp sorted set
				storeKeysInTempSortedSet(newKeys, tempSortedSet);

				// Move to the next page
				cursor = scanResult.getCursor();
			} while (!scanResult.isCompleteIteration());

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				jedis.close();
				indexWriter.close();
				luceneIndexDirectory.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		}
	}
	public static void main(String[] args) {
        System.out.println("Started the indexer");

		try {
            // Subscribe to the "channel1"
            JedisPubSub jedisPubSub = new JedisPubSub() {
                @Override
                public void onMessage(String channel, String message) {
                    // System.out.println("Received message from " + channel + ": " + message);
                    // Check if the termination message is received
                    if ("STOP_LISTENING".equals(message)) {
                        // System.out.println("Received termination message. Exiting.");
                        unsubscribe();  
                        // jedis.close();  // Close the connection
                        return;
                    }
                    try {
						indexDataIntoLucene(indexWriter, message);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

                }
            };

            jedis.subscribe(jedisPubSub, "channel1");
        } catch (Exception e) {
            // Handle exceptions, e.g., print an error message
            System.err.println("Error occurred: " + e.getMessage());
        } finally {
			try {
				jedis.close();
				indexWriter.close();
				luceneIndexDirectory.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		}      
	}
}
