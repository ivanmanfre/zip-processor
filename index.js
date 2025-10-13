const unzipper = require('unzipper');
const xml2js = require('xml2js');
const multer = require('multer');
const upload = multer({ storage: multer.memoryStorage() });

module.exports = async (req, res) => {
  // Handle file upload
  upload.single('file')(req, res, async (err) => {
    if (err) {
      return res.status(500).json({ error: err.message });
    }

    try {
      const parser = new xml2js.Parser();
      const xmlFiles = [];
      const { Readable } = require('stream');
      const stream = Readable.from(req.file.buffer);

      await stream
        .pipe(unzipper.Parse())
        .on('entry', async (entry) => {
          if (entry.type === 'File' && entry.path.toLowerCase().endsWith('.xml')) {
            const chunks = [];
            await new Promise((resolve) => {
              entry
                .on('data', (chunk) => chunks.push(chunk))
                .on('end', async () => {
                  try {
                    const xml = Buffer.concat(chunks).toString('utf8');
                    const parsed = await parser.parseStringPromise(xml);
                    xmlFiles.push({ fileName: entry.path, data: parsed });
                  } catch (e) {}
                  resolve();
                });
            });
          } else {
            entry.autodrain();
          }
        })
        .promise();

      res.json({ 
        success: true, 
        filesProcessed: xmlFiles.length, 
        data: xmlFiles 
      });
    } catch (error) {
      res.status(500).json({ error: error.message });
    }
  });
};
