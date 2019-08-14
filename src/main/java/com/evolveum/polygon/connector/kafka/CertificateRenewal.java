/**
 * Copyright (c) 2010-2019 Evolveum
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.evolveum.polygon.connector.kafka;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.security.KeyStore;
import java.security.KeyStore.PrivateKeyEntry;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.UnrecoverableEntryException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.common.security.GuardedString.Accessor;
import org.identityconnectors.framework.common.exceptions.ConnectorIOException;
import org.json.JSONObject;

/**
 * @author skublik
 */
public class CertificateRenewal {
	
	private static final Log LOGGER = Log.getLog(KafkaConnector.class);
	
	private static final String PASSWORD_FOR_GENERATION = "passwordForGen123";
	private static final String DEFAULT_ALLIAS_PREFIX_CERTIFICATE = "caroot";
	
	private KafkaConfiguration configuration;
	private File tempFile = null;
	private ZipFile zip = null;
	
	public enum LifeState {
	    NOT_VALID_ENTRY,
	    NOT_EXIST,
	    VALID
	}
	
	public CertificateRenewal(KafkaConfiguration configuration) {
		this.configuration = configuration;
	}
	
	private boolean existFile(String path) {
		File f = new File(path);
		return (f.exists() && f.isFile());
	}
	
	public LifeState isPrivateKeyExpiredOrNotExist(){
		if(!existFile(configuration.getSslKeyStorePath())) {
			return LifeState.NOT_EXIST;
		}
		String clearPass = getClearPassword(configuration.getSslKeyStorePassword());
		
		KeyStore ks;
		try {
			ks = loadKeyStoreType(configuration.getSslKeyStoreType(), configuration.getSslKeyStorePath(),
					clearPass, null);
		} catch (NoSuchAlgorithmException | CertificateException | KeyStoreException | IOException e) {
			LOGGER.warn(e, "Couldn't load keyStore from " + configuration.getSslKeyStorePath() + " with set password");
			return LifeState.NOT_EXIST;
		}
			
		String privateKeyAlias = configuration.getSslPrivateKeyEntryAlias();
		if(StringUtils.isBlank(privateKeyAlias)) {
			privateKeyAlias = configuration.getUsernameRenewal();
		}
		try {
			GuardedString privateKeyPassword = configuration.getSslPrivateKeyEntryPassword();
			final StringBuilder clearprivateKeyPassword = new StringBuilder();
			if (privateKeyPassword != null) {
				Accessor accessor = new GuardedString.Accessor() {
					@Override
					public void access(char[] chars) {
						clearprivateKeyPassword.append(new String(chars));
					}
				};
				privateKeyPassword.access(accessor);
			}
			KeyStore.ProtectionParameter entryPassword =
			        new KeyStore.PasswordProtection(clearprivateKeyPassword.toString().toCharArray());
			
			if(!ks.containsAlias(privateKeyAlias)) {
				return LifeState.NOT_VALID_ENTRY;
			}
			KeyStore.PrivateKeyEntry privateKeyEntry = (KeyStore.PrivateKeyEntry)
					ks.getEntry(privateKeyAlias, entryPassword);
			
			X509Certificate cert = ((X509Certificate)privateKeyEntry.getCertificate());
			if(cert == null) {
				throw new IllegalArgumentException("Certificate from private key is null");
			}
			Date expired = cert.getNotAfter();
			if(expired == null) {
				throw new IllegalArgumentException("Expired Date from private key certificate is null");
			}
			LOGGER.ok("Expiration date for private key is " + expired);
			Integer intervalForCertificateRenewal = configuration.getIntervalForCertificateRenewal();
			Date actualTime = new Date(System.currentTimeMillis() + 
					(intervalForCertificateRenewal == null ? 0 : TimeUnit.MINUTES.toMillis(intervalForCertificateRenewal)));
			if(expired.before(actualTime)) {
				return LifeState.NOT_VALID_ENTRY;
			};
			return LifeState.VALID;
		} catch (NoSuchAlgorithmException | UnrecoverableEntryException | KeyStoreException e) {
			LOGGER.warn(e, "Couldn't load privateKey with alias " + privateKeyAlias, e);
			return LifeState.NOT_VALID_ENTRY;
		}
	}
	
	private String getClearPassword(GuardedString keyStorePassword) {
		final StringBuilder clearKeyStorePassword = new StringBuilder();
		if (keyStorePassword != null) {
			Accessor accessor = new GuardedString.Accessor() {
				@Override
				public void access(char[] chars) {
					clearKeyStorePassword.append(new String(chars));
				}
			};
			keyStorePassword.access(accessor);
		}
		return clearKeyStorePassword.toString();
	}
	
	private KeyStore loadKeyStoreType(String type, String path, String clearKeyStorePassword, LifeState state) throws NoSuchAlgorithmException, CertificateException, IOException, KeyStoreException {
		if(StringUtils.isBlank(type)) {
			type = "JKS";
		}
		KeyStore ks = null;
			ks = KeyStore.getInstance(type);
		
			FileInputStream is = null;
			if(state == null || !state.equals(LifeState.NOT_EXIST)) {
				is = new FileInputStream(path);
			}
			ks.load(is, clearKeyStorePassword.toString().toCharArray());
		
		return ks;
	}

	//return true is expired and false is needed create new file (file not exist or is broken)
	public LifeState isTrustCertificateExpiredOrNotExist(){
		if(!existFile(configuration.getSslTrustStorePath())) {
			return LifeState.NOT_EXIST;
		}
		String clearPass = getClearPassword(configuration.getSslTrustStorePassword());
		
		KeyStore ks;
		try {
			ks = loadKeyStoreType(configuration.getSslTrustStoreType(), configuration.getSslTrustStorePath(),
					clearPass, null);
		} catch (NoSuchAlgorithmException | CertificateException | KeyStoreException | IOException e) {
			LOGGER.warn(e, "Couldn't load trustStore from " + configuration.getSslTrustStorePath() + " with set password");
			return LifeState.NOT_EXIST;
		}
		
		String certAliasPrefix = configuration.getSslTrustCertificateAliasPrefix();
		if(StringUtils.isBlank(certAliasPrefix)) {
			certAliasPrefix = DEFAULT_ALLIAS_PREFIX_CERTIFICATE;
		}
			
		
		int i = 0;
		try {
			while(!ks.containsAlias(certAliasPrefix+i)) {
				String certAlias = certAliasPrefix+i;
				X509Certificate cert = null;
				cert = ((X509Certificate)ks.getCertificate(certAlias));
				
				 
				if(cert == null) {
					throw new IllegalArgumentException("Certificate with alias "+certAlias+" is null");
				}
				Date expired = cert.getNotAfter();
				if(expired == null) {
					throw new IllegalArgumentException("Expired Date from trust certificate is null");
				}
				LOGGER.ok("Expiration date for trust certificate is " + expired);
				Integer intervalForCertificateRenewal = configuration.getIntervalForCertificateRenewal();
				Date actualTime = new Date(System.currentTimeMillis() + 
						(intervalForCertificateRenewal == null ? 0 : TimeUnit.MINUTES.toMillis(intervalForCertificateRenewal)));
				if(expired.before(actualTime)) {
					return LifeState.NOT_VALID_ENTRY;
				};
				i++;
			}
		} catch (KeyStoreException e) {
			LOGGER.error(e, "Couldn't load certificate with alias " + certAliasPrefix+i, e);
			return LifeState.NOT_VALID_ENTRY;
		}
		if(i == 0) {
			return LifeState.NOT_VALID_ENTRY;
		}
		return LifeState.VALID;
	}
	
	public void renewalPrivateKeyAndCert(LifeState privateKeyState, LifeState certState) {
		String clearKeyStorePass = getClearPassword(configuration.getSslKeyStorePassword());
		KeyStore keyStore = null;
		try {
			keyStore = loadKeyStoreType(configuration.getSslKeyStoreType(), configuration.getSslKeyStorePath(),
					clearKeyStorePass, privateKeyState);
		} catch (NoSuchAlgorithmException | CertificateException | IOException | KeyStoreException e) {
			LOGGER.error(e, "Couldn't load keyStore from " + configuration.getSslKeyStorePath() + " with set password");
		}
		
		String clearPass = getClearPassword(configuration.getSslTrustStorePassword());
		KeyStore trustStore = null;
		try {
			trustStore = loadKeyStoreType(configuration.getSslTrustStoreType(), configuration.getSslTrustStorePath(),
					clearPass, certState);
		} catch (NoSuchAlgorithmException | CertificateException | IOException | KeyStoreException e) {
			LOGGER.error(e, "Couldn't load trustStore from " + configuration.getSslTrustStorePath() + " with set password");
		}
		
		KeyStore privateKey = null;
		try {
			privateKey = getcontextOfP12File();
		} catch (KeyStoreException e) {
			LOGGER.error(e, "Couldn't create instance of keyStore with type PKCS12");
		} catch (NoSuchAlgorithmException | CertificateException | IOException e) {
			LOGGER.error(e, "Couldn't load KeyStore from received inputStream");
		}
		
		if (privateKey == null){
			throw new IllegalArgumentException("Failed to get new privateKey and certificate");
		}
		
		KeyStore.ProtectionParameter entryPassword =
		        new KeyStore.PasswordProtection(PASSWORD_FOR_GENERATION.toCharArray());
		
		KeyStore.PrivateKeyEntry keyEntry = null;
		try {
			keyEntry = (PrivateKeyEntry) privateKey.getEntry(configuration.getUsernameRenewal(), entryPassword);
		} catch (NoSuchAlgorithmException | UnrecoverableEntryException | KeyStoreException e) {
			LOGGER.error(e, "Couldn't get PrivateKeyEntry from created KeyStore");
		}
		
		List<X509Certificate> trustCerts = new ArrayList<X509Certificate>();
		List<Certificate> primaryKeyCerts = new ArrayList<Certificate>();
		PrivateKey key = keyEntry.getPrivateKey();
		
		for (Certificate cert : keyEntry.getCertificateChain()) {
			if(cert instanceof X509Certificate) {
				String owner = ((X509Certificate)cert).getSubjectDN().getName().split(",")[0].replace("cn=", "").replace("CN=", "");
				if(owner.equalsIgnoreCase(configuration.getUsernameRenewal())) {
					primaryKeyCerts.add(cert);
				} else {
					trustCerts.add((X509Certificate) cert);
				}
			}
		}
		
		GuardedString privateKeyPassword = configuration.getSslPrivateKeyEntryPassword();
		final StringBuilder clearprivateKeyPassword = new StringBuilder();
		if (privateKeyPassword != null) {
			Accessor accessor = new GuardedString.Accessor() {
				@Override
				public void access(char[] chars) {
					clearprivateKeyPassword.append(new String(chars));
				}
			};
			privateKeyPassword.access(accessor);
		}
		
		try {
			keyStore.setKeyEntry(configuration.getSslPrivateKeyEntryAlias(), key,
					clearprivateKeyPassword.toString().toCharArray(), primaryKeyCerts.toArray(new Certificate[primaryKeyCerts.size()]) );
		} catch (KeyStoreException e) {
			LOGGER.error(e, "Couldn't set new PrivateKeyEntry to KeyStore");
		}
		
		String certAliasPrefix = configuration.getSslTrustCertificateAliasPrefix();
		if(StringUtils.isBlank(certAliasPrefix)) {
			certAliasPrefix = DEFAULT_ALLIAS_PREFIX_CERTIFICATE;
		}
		
		int i = 0;
		for(X509Certificate trustCert : trustCerts) {
			String certAlias = certAliasPrefix + i;
			try {
				trustStore.setCertificateEntry(certAlias, trustCert);
			} catch (KeyStoreException e) {
				LOGGER.error(e, "Couldn't set new X509Certificate to TrustStore");
			}
			
			try {
				keyStore.setCertificateEntry(certAlias, trustCert);
			} catch (KeyStoreException e) {
				LOGGER.error(e, "Couldn't set new X509Certificate to KeyStore");
			}
			i++;
		}
	
		try (FileOutputStream keyStoreOutputStream = new FileOutputStream(configuration.getSslTrustStorePath())) {
			trustStore.store(keyStoreOutputStream, clearPass.toCharArray());
		} catch (KeyStoreException | NoSuchAlgorithmException | CertificateException | IOException e) {
			LOGGER.error(e, "Couldn't save trustStore with new X509Certificate to filesystem");
		}
		
		try (FileOutputStream keyStoreOutputStream = new FileOutputStream(configuration.getSslKeyStorePath())) {
		    keyStore.store(keyStoreOutputStream, clearKeyStorePass.toCharArray());
		} catch (KeyStoreException | NoSuchAlgorithmException | CertificateException | IOException e) {
			LOGGER.error(e, "Couldn't save keyStore with new privateKey to filesystem");
		}
		
		if(zip != null) {
			try {
				zip.close();
			} catch (IOException e) {
				LOGGER.error(e, "Couldn't close zip archive");
			}
		}
		if(tempFile != null) {
			tempFile.delete();
		}
	}
	
	private void responseClose(CloseableHttpResponse response) {
		try {
			response.close();
		} catch (IOException e) {
			StringBuilder sb = new StringBuilder();
			sb.append("Failed close response: ").append(response);
			LOGGER.warn(e, sb.toString());
		}
	}
	
	private KeyStore getcontextOfP12File() throws IOException, NoSuchAlgorithmException, CertificateException, KeyStoreException {
		CloseableHttpClient httpclient = HttpClientBuilder.create().build();
		
		List<NameValuePair> pairs = new ArrayList<NameValuePair>();
		pairs.add(new BasicNameValuePair("grant_type", "password"));
		pairs.add(new BasicNameValuePair("username", configuration.getUsernameRenewal()));
		
		GuardedString password = configuration.getPasswordRenewal();
		final StringBuilder clearPassword = new StringBuilder();
		if (password != null) {
			Accessor accessor = new GuardedString.Accessor() {
				@Override
				public void access(char[] chars) {
					clearPassword.append(new String(chars));
				}
			};
			password.access(accessor);
		}
		
		pairs.add(new BasicNameValuePair("password", clearPassword.toString()));
		pairs.add(new BasicNameValuePair("client_id", configuration.getClientIdRenewal()));
		
		HttpPost request;
		request = new HttpPost(configuration.getSsoUrlRenewal());
		request.addHeader("Content-type", "application/x-www-form-urlencoded");
		try {
			request.setEntity(new UrlEncodedFormEntity(pairs));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} 
		
		CloseableHttpResponse response = httpclient.execute(request);
		
		HttpEntity responseEntity = response.getEntity();
		JSONObject json = null;
		try {
			byte[] byteResult = EntityUtils.toByteArray(responseEntity);
			String result = new String(byteResult, "UTF-8");
			responseClose(response);
			json = new JSONObject(result);
		} catch (IOException e) {
			StringBuilder sb = new StringBuilder();
			sb.append("Failed creating result from HttpEntity: ").append(responseEntity).append(";")
					.append(e.getLocalizedMessage());
			responseClose(response);
			throw new ConnectorIOException(sb.toString(), e);
		}
		
		String accessToken = json.getString("access_token");
		
		pairs.clear();
		pairs.add(new BasicNameValuePair("format", "p12"));
		pairs.add(new BasicNameValuePair("password", PASSWORD_FOR_GENERATION));
		request = new HttpPost(configuration.getServiceUrlRenewal());
		request.addHeader("Authorization", "Bearer " + accessToken);
		request.setEntity(new UrlEncodedFormEntity(pairs));
		response = httpclient.execute(request);
		
		responseEntity = response.getEntity();
		InputStream zipStream = responseEntity.getContent();
		
		tempFile = File.createTempFile("tempfile", ".tmp");
		try(OutputStream outputStream = new FileOutputStream(tempFile)){
		    IOUtils.copy(zipStream, outputStream);
		} catch (IOException e) {
			LOGGER.warn(e, "Couldn't copy InputStream of received zip file to tmp file");
		}
		
	    zip = new ZipFile(tempFile);
	    Enumeration zipFileEntries = zip.entries();
	    ZipEntry p12Entry = null;
	    while (zipFileEntries.hasMoreElements())
	    {
	        ZipEntry entry = (ZipEntry) zipFileEntries.nextElement();
	        String currentEntry = entry.getName();
	        LOGGER.ok("Parsing file " +currentEntry+ " from zip archive");
	        if(currentEntry.endsWith(".p12")) {
	        	p12Entry = entry;
	        }
	    }
	    LOGGER.ok(".p12 entry from zip archive " +p12Entry);
		if(p12Entry != null) {
			InputStream retIs = zip.getInputStream(p12Entry);
			KeyStore privateKey = KeyStore.getInstance("PKCS12");
			privateKey.load(retIs, PASSWORD_FOR_GENERATION.toCharArray());
			LOGGER.ok("PKCS12 KeyStrore from zip archive " +privateKey);
			return privateKey;
		}
		return null;
	}
}
