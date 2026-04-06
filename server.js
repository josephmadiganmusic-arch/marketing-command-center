const express = require('express');
const fs = require('fs');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

app.get('/', (req, res) => {
  let html = fs.readFileSync(path.join(__dirname, 'MARKETING-COMMAND-CENTER.html'), 'utf8');
  html = html.replace('%%CLAUDE_API_KEY%%', process.env.CLAUDE_API_KEY || '');
  res.type('html').send(html);
});

app.use(express.static(__dirname));

app.listen(PORT, () => {
  console.log(`Marketing Command Center running on port ${PORT}`);
});
