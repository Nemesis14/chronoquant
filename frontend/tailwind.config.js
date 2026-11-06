/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,ts,jsx,tsx}'],
  theme: {
    extend: {
      colors: {
        cq: {
          bg: '#0b0e11',
          panel: '#111418',
          grid: '#2b3139',
          text: '#eaecef',
          muted: '#848e9c',
          green: '#0ecb81',
          red: '#f6465d',
          gold: '#f0b90b',
          blue: '#5b8af5',
        },
      },
    },
  },
  plugins: [],
}
