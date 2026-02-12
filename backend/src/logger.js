function ts() {
  return new Date().toISOString();
}

function line(level, msg, meta) {
  const base = `[${ts()}] [${level}] ${msg}`;
  if (!meta) return base;
  return `${base} ${JSON.stringify(meta)}`;
}

export const logger = {
  info(msg, meta) {
    console.log(line('INFO', msg, meta));
  },
  warn(msg, meta) {
    console.warn(line('WARN', msg, meta));
  },
  error(msg, meta) {
    console.error(line('ERROR', msg, meta));
  },
};
