// File: index.js
const { 
  S3Client,
  PutObjectCommand, 
  GetObjectCommand, 
  ListObjectsV2Command
} = require('@aws-sdk/client-s3');
const Busboy = require('@fastify/busboy');

// Initialize the S3 client
const s3Client = new S3Client({ region: process.env.AWS_REGION || 'us-east-1' });
const BUCKET_NAME = process.env.BUCKET_NAME;
const ALLOWED_EXTENSIONS = ['.docx', '.pdf', '.jpg', '.png', '.jpeg', '.txt', '.xlsx'];

// Enhanced CORS headers function
function getCorsHeaders() {
  return {
    'Access-Control-Allow-Origin': '*', // For production, use your specific domain
    'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Requested-With,Accept',
    'Access-Control-Allow-Credentials': 'true',
    'Access-Control-Max-Age': '86400', // 24 hours
  };
}

// Simple function to extract file and field data from multipart form data
async function extractFileFromForm(event) {
  return new Promise((resolve, reject) => {
    try {
      // Get content type from headers
      const contentType = event.headers['content-type'] || event.headers['Content-Type'];
      
      // Verify we have multipart/form-data
      if (!contentType || !contentType.includes('multipart/form-data')) {
        console.log('Not a multipart/form-data request:', contentType);
        return reject(new Error('Not a multipart/form-data request'));
      }
      
      console.log('Processing multipart/form-data with content type:', contentType);
      
      // Set up busboy to parse the multipart form
      const bb = new Busboy({ headers: { 'content-type': contentType } });
      
      let fileData = null;
      let fileName = '';
      let fileType = '';
      
      // Initialize object to store form fields
      const formFields = {
        documentValueCode: '',
        documentValueTypeCode: ''
      };
      
      // Handle file parts
      bb.on('file', (fieldname, fileStream, filename, encoding, mimetype) => {
        console.log(`Found file in form: ${filename}, type: ${mimetype}`);
        
        fileName = filename;
        fileType = mimetype;
        
        // Collect file data chunks
        const chunks = [];
        fileStream.on('data', (data) => {
          chunks.push(data);
        });
        
        fileStream.on('end', () => {
          fileData = Buffer.concat(chunks);
          console.log(`File data collected: ${fileData.length} bytes`);
        });
      });
      
      // Handle text fields
      bb.on('field', (fieldname, val) => {
        console.log(`Form field: ${fieldname} = ${val}`);
        
        // Save specific fields we're interested in
        if (fieldname === 'documentValueCode') {
          formFields.documentValueCode = val;
        } else if (fieldname === 'documentValueTypeCode') {
          formFields.documentValueTypeCode = val;
        }
      });
      
      // Handle completion
      bb.on('finish', () => {
        if (!fileData) {
          return reject(new Error('No file found in form data'));
        }
        
        resolve({
          fileData,
          fileName,
          fileType,
          documentValueCode: formFields.documentValueCode,
          documentValueTypeCode: formFields.documentValueTypeCode
        });
      });
      
      // Handle parsing errors
      bb.on('error', (error) => {
        console.error('Error parsing form data:', error);
        reject(error);
      });
      
      // Process the request body
      if (event.isBase64Encoded) {
        // Decode base64 encoded body
        const decodedBody = Buffer.from(event.body, 'base64');
        bb.write(decodedBody);
      } else {
        // Use body directly if not encoded
        bb.write(Buffer.from(event.body));
      }
      
      bb.end();
    } catch (error) {
      console.error('Exception processing form data:', error);
      reject(error);
    }
  });
}

// Main handler to route requests based on action
exports.handler = async (event) => {
  try {
    console.log('Received event:', JSON.stringify({
      httpMethod: event.httpMethod,
      path: event.path,
      queryStringParameters: event.queryStringParameters,
      headers: {
        'content-type': event.headers['content-type'] || event.headers['Content-Type']
      },
      isBase64Encoded: event.isBase64Encoded,
      bodyLength: event.body ? event.body.length : 0
    }));
    
    // Get CORS headers
    const headers = getCorsHeaders();
    
    // Special handling for OPTIONS requests (preflight)
    if (event.httpMethod === 'OPTIONS') {
      console.log('Handling OPTIONS preflight request');
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({ message: 'Preflight request successful' }),
      };
    }
    
    // Route based on the action parameter from query string
    const action = event.queryStringParameters?.action;
    
    switch (action) {
      case 'uploadFile':
        return await uploadFile(event, headers);
      case 'listDocuments':
        return await listDocuments(headers);
      case 'downloadFile':
        return await downloadFile(event, headers);
      default:
        return {
          statusCode: 400,
          headers,
          body: JSON.stringify({ error: 'Invalid action specified' }),
        };
    }
  } catch (error) {
    console.error('Error:', error);
    return {
      statusCode: 500,
      headers: getCorsHeaders(),
      body: JSON.stringify({ error: 'Internal server error', details: error.message }),
    };
  }
};

// File upload with proper multipart form parsing
async function uploadFile(event, headers) {
  console.log('Processing file upload request');
  
  try {
    // Extract file from multipart form
    const { 
      fileData, 
      fileName, 
      fileType, 
      documentValueCode,
      documentValueTypeCode
    } = await extractFileFromForm(event);
    
    // Log the form field values
    console.log(`Form fields - documentValueCode: ${documentValueCode}, documentValueTypeCode: ${documentValueTypeCode}`);
    
    // Validate file extension
    const fileExtension = `.${fileName.split('.').pop().toLowerCase()}`;
    if (!ALLOWED_EXTENSIONS.includes(fileExtension)) {
      return {
        statusCode: 400,
        headers,
        body: JSON.stringify({ 
          error: 'Invalid file type', 
          message: `Supported file types: ${ALLOWED_EXTENSIONS.join(', ')}` 
        }),
      };
    }
    
    // Determine final content type
    let contentType = fileType;
    
    // Override content type for well-known extensions
    switch (fileExtension.toLowerCase()) {
      case '.pdf':
        contentType = 'application/pdf';
        break;
      case '.docx':
        contentType = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document';
        break;
      case '.xlsx':
        contentType = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet';
        break;
      case '.txt':
        contentType = 'text/plain';
        break;
      case '.jpg':
      case '.jpeg':
        contentType = 'image/jpeg';
        break;
      case '.png':
        contentType = 'image/png';
        break;
    }
    
    // Create a unique key for the file
    const key = `documents/${Date.now()}-${fileName}`;
    
    // Log upload details
    console.log(`Uploading file: ${fileName}, Content-Type: ${contentType}, Size: ${fileData.length} bytes`);
    
    // Include metadata with the document codes
    const metadata = {
      documentValueCode: documentValueCode || 'NA',
      documentValueTypeCode: documentValueTypeCode || 'NA'
    };
    
    // Upload to S3
    const uploadParams = {
      Bucket: BUCKET_NAME,
      Key: key,
      Body: fileData,
      ContentType: contentType,
      Metadata: metadata  // Include the form field values as metadata
    };
    
    const command = new PutObjectCommand(uploadParams);
    await s3Client.send(command);
    
    console.log(`File uploaded successfully to ${key} with metadata`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        message: 'File uploaded successfully',
        key,
        fileName,
        documentValueCode,
        documentValueTypeCode
      }),
    };
  } catch (error) {
    console.error('Error uploading file to S3:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ error: 'Failed to upload file', details: error.message }),
    };
  }
}

// List all documents in the bucket
async function listDocuments(headers) {
  const command = new ListObjectsV2Command({
    Bucket: BUCKET_NAME,
    Prefix: 'documents/'
  });
  
  const response = await s3Client.send(command);
  
  const documents = response.Contents ? response.Contents.map(item => {
    // Extract the filename from the key
    const key = item.Key;
    const fileName = key.split('/').pop();
    
    return {
      key: item.Key,
      fileName,
      size: item.Size,
      lastModified: item.LastModified,
    };
  }) : [];
  
  return {
    statusCode: 200,
    headers,
    body: JSON.stringify({ documents }),
  };
}

// Direct file download from S3
async function downloadFile(event, headers) {
  const key = event.queryStringParameters?.key;
  
  if (!key) {
    return {
      statusCode: 400,
      headers,
      body: JSON.stringify({ error: 'Document key is required' }),
    };
  }
  
  try {
    // Fetch the file from S3
    const command = new GetObjectCommand({
      Bucket: BUCKET_NAME,
      Key: key
    });
    
    const response = await s3Client.send(command);
    
    // Extract file name from key
    const fileName = key.split('/').pop();
    
    // Get content type from S3
    const contentType = response.ContentType || 'application/octet-stream';
    
    // Get metadata if available
    const metadata = response.Metadata || {};
    console.log('Document metadata:', metadata);
    
    // Convert the readable stream to buffer
    const fileStream = response.Body;
    const chunks = [];
    
    for await (const chunk of fileStream) {
      chunks.push(chunk);
    }
    
    const fileBuffer = Buffer.concat(chunks);
    console.log(`Downloaded file: ${fileName}, Size: ${fileBuffer.length} bytes, Type: ${contentType}`);
    
    // Check if we need to handle this file differently based on content type
    const isTextPlainFile = contentType === 'text/plain';
    
    if (isTextPlainFile) {
      // For text/plain files, we'll handle differently since it's not in binary media types
      // Return as JSON with the file content
      return {
        statusCode: 200,
        headers: {
          ...headers,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          fileName,
          contentType,
          fileContent: fileBuffer.toString('base64'),
          isBase64Encoded: true,
          metadata  // Include the metadata in the response
        })
      };
    } else {
      // For binary files, return as before
      // Set response headers for file download
      const downloadHeaders = {
        ...headers,
        'Content-Type': contentType,
        'Content-Disposition': `attachment; filename="${fileName}"`,
        'Content-Length': fileBuffer.length.toString(),
        // Add metadata as custom headers
        'X-Document-Value-Code': metadata.documentvaluecode || 'NA',
        'X-Document-Value-Type-Code': metadata.documentvaluetypecode || 'NA'
      };
      
      // Return file content directly, base64 encoded
      return {
        statusCode: 200,
        headers: downloadHeaders,
        body: fileBuffer.toString('base64'),
        isBase64Encoded: true
      };
    }
  } catch (error) {
    console.error('Error downloading file:', error);
    
    if (error.name === 'NoSuchKey') {
      return {
        statusCode: 404,
        headers,
        body: JSON.stringify({ error: 'Document not found' }),
      };
    }
    
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ error: 'Failed to download file', details: error.message }),
    };
  }
}