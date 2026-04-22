const express = require('express');
const multer = require('multer');
const unzipper = require('unzipper');
const xml2js = require('xml2js');
const ftp = require('basic-ftp');
const { Readable } = require('stream');

const app = express();
app.use(express.json());
const upload = multer({ storage: multer.memoryStorage() });
const parser = new xml2js.Parser();

app.get('/', (req, res) => {
  res.json({ status: 'Zip processor is running' });
});

app.post('/process', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: 'No file uploaded' });
    }
    const stream = Readable.from(req.file.buffer);
    const data = await parseZipStream(stream);
    res.json({ success: true, filesProcessed: data.length, data });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: error.message });
  }
});

app.post('/process-ftp', async (req, res) => {
  const client = new ftp.Client(0);
  client.ftp.verbose = false;
  try {
    const date = req.body?.date || formatToday();
    const remotePath = req.body?.path || `/Xpress_105622/1.4_DL_XpressSW3P_XML_${date}.zip`;

    if (!process.env.FTP_HOST || !process.env.FTP_USER || !process.env.FTP_PASSWORD) {
      return res.status(500).json({ error: 'FTP_HOST / FTP_USER / FTP_PASSWORD env vars not set' });
    }

    await client.access({
      host: process.env.FTP_HOST,
      user: process.env.FTP_USER,
      password: process.env.FTP_PASSWORD,
      secure: process.env.FTP_SECURE === 'true',
    });

    const zipStream = unzipper.Parse();
    const entriesPromise = collectXmlEntries(zipStream);

    await client.downloadTo(zipStream, remotePath);
    const data = await entriesPromise;

    res.json({ success: true, filesProcessed: data.length, source: remotePath, data });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: error.message });
  } finally {
    client.close();
  }
});

function formatToday() {
  const d = new Date();
  const pad = (n) => String(n).padStart(2, '0');
  return `${d.getFullYear()}${pad(d.getMonth() + 1)}${pad(d.getDate())}`;
}

async function parseZipStream(readable) {
  const zipStream = unzipper.Parse();
  const entriesPromise = collectXmlEntries(zipStream);
  readable.pipe(zipStream);
  return entriesPromise;
}

function collectXmlEntries(zipStream) {
  return new Promise((resolve, reject) => {
    const xmlFiles = [];
    const pending = [];

    zipStream.on('entry', (entry) => {
      if (entry.type === 'File' && entry.path.toLowerCase().endsWith('.xml')) {
        const chunks = [];
        pending.push(
          new Promise((resolveEntry) => {
            entry
              .on('data', (chunk) => chunks.push(chunk))
              .on('end', async () => {
                try {
                  const xml = Buffer.concat(chunks).toString('utf8');
                  const parsed = await parser.parseStringPromise(xml);
                  xmlFiles.push({ fileName: entry.path, data: cleanProjectData(parsed) });
                } catch (e) {
                  console.error('Parse error:', e);
                }
                resolveEntry();
              })
              .on('error', resolveEntry);
          })
        );
      } else {
        entry.autodrain();
      }
    });

    zipStream.on('close', async () => {
      try {
        await Promise.all(pending);
        resolve(xmlFiles);
      } catch (e) {
        reject(e);
      }
    });
    zipStream.on('error', reject);
  });
}

function cleanProjectData(data) {
  if (!data.Projects || !data.Projects.Project) {
    return data;
  }

  const projects = Array.isArray(data.Projects.Project)
    ? data.Projects.Project
    : [data.Projects.Project];

  const cleanedProjects = projects.map((project) => ({
    projectId: project.$?.ProjectID,
    title: project.$?.Title,
    stage: project.$?.Stage,
    url: project.$?.URL,
    updateDate: project.$?.UpdateDate,
    updateText: project.$?.UpdateText,
  }));

  return { projects: cleanedProjects };
}

const PORT = process.env.PORT || 3080;
app.listen(PORT, '0.0.0.0', () => {
  console.log(`Server running on port ${PORT}`);
});
