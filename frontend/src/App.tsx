import { BrowserRouter, Routes, Route } from 'react-router-dom'
import Layout from './components/Layout'
import { AssetDashboard, RootRedirect } from './components/AssetRoutes'
import ReportsPage from './pages/ReportsPage'
import ChatPage from './pages/ChatPage'

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Layout />}>
          <Route index element={<RootRedirect />} />
          <Route path="a/:asset" element={<AssetDashboard />} />
          <Route path="reports" element={<ReportsPage />} />
          <Route path="chat" element={<ChatPage />} />
        </Route>
      </Routes>
    </BrowserRouter>
  )
}
