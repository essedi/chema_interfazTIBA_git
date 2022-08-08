/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package interfaztiba;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbFile;
import jcifs.smb.SmbFileInputStream;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.Namespace;
import org.jdom2.input.SAXBuilder;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;

/**
 *
 * @author RUBEN.SEGARRA
 */
public class InterfazTIBA {

    private static final Logger LOGGER = Logger.getLogger(InterfazTIBA.class.getName());
    private static String postgresqlIp = "localhost";
    private static String postgresqlPort = "5432";
    private static String postgresqlDatabaseName = "c_ballester";
    private static String postgresqlUser = "tad";
    private static String postgresqlPassword = "tad";
    private static String ftpIp = "51.77.235.1";
    private static String ftpPort = "21";
    private static String ftpUser = "cballester";
    private static String ftpPassword = "udfv8NwCOFJsWICb";
    
    private static final String POSTGRESQL_URL = "jdbc:postgresql://" + postgresqlIp + ":" + postgresqlPort + "/"
            + postgresqlDatabaseName + "?user=" + postgresqlUser + "&password=" + postgresqlPassword;
    
    private static String imagePath = "/home/exos/planificador/cbdriver/images/estados/";

    private static final File textFile = new File("./tmp/Datos_ChemaBallester_" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmm")) + ".txt");
//    private static final File textFile = new File("\\" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmm")) + ".txt");
    
    private static final Map<String, List<String>> ultimosEventos = new HashMap<>();
    private static final Map<String, List<String>> imagenesNameFile = new HashMap<>();

    public static void main(String[] args) {
        inicio();
        loadProperties();
        createTextFile();
        anyadirEstadosCBDriver();
        anyadirEstadosPosicion();
        sendTextFileToTiba();
        sendImagesToTiba();
        updateEstadosEnviados();
    }

    private static void inicio() {
        try {
            File folder = new File("./logs");
            if (!folder.exists() || !folder.isDirectory()) {
                System.out.println("creando ruta" + folder.toString());
                folder.mkdirs();
            }
            File folder2 = new File("./enviado");
            if (!folder2.exists() || !folder2.isDirectory()) {
                System.out.println("creando ruta" + folder2.toString());
                folder2.mkdirs();
            }
            File folder3 = new File("./enviado_img");
            if (!folder3.exists() || !folder3.isDirectory()) {
                System.out.println("creando ruta" + folder3.toString());
                folder3.mkdirs();
            }
            InputStream is = InterfazTIBA.class.getResourceAsStream("log.properties");
            LogManager.getLogManager().readConfiguration(is);

        } catch (IOException ex) {
            Logger.getLogger(InterfazTIBA.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SecurityException ex) {
            Logger.getLogger(InterfazTIBA.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private static void loadProperties() {
        try {
            Properties properties = new Properties();
            properties.load(new FileReader("./config/config.properties"));
            postgresqlIp = properties.getProperty("ip", "192.168.0.7");
            postgresqlPort = properties.getProperty("port", "5432");
            postgresqlDatabaseName = properties.getProperty("db", "c_ballester");
            postgresqlUser = properties.getProperty("user", "tad");
            postgresqlPassword = properties.getProperty("pass", "tad");
            ftpIp = properties.getProperty("ftpIp", "13.69.139.244");
            ftpPort = properties.getProperty("ftpPort", "21");
            ftpUser = properties.getProperty("ftpUser", "cballester");
            ftpPassword = properties.getProperty("ftpPssword", "NabwR2Wzy7nXe9n");
            imagePath = String.valueOf(properties.getProperty("imagePath", ""));
        } catch (FileNotFoundException ex) {
            LOGGER.log(Level.SEVERE, "", ex);
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, "", ex);
        }
    }

    private static void createTextFile() {
        try {
            textFile.getParentFile().mkdirs();
            textFile.createNewFile();
        } catch (IOException ex) {
            Logger.getLogger(InterfazTIBA.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private static void anyadirEstadosCBDriver() {
        try {
            String postgresqlUrl = "jdbc:postgresql://" + postgresqlIp + ":" + postgresqlPort + "/"
                    + postgresqlDatabaseName + "?user=" + postgresqlUser + "&password=" + postgresqlPassword;

            Connection postgresqlConnection = DriverManager.getConnection(postgresqlUrl);

            String selectSql = "SELECT distinct r.codigo, r.expediente_ol, r.numero_contenedor1, r.numero_contenedor2, e.tipo_aux, "
                    + "v.matricula, e.latitud, e.longitud, to_char(e.f_inicio, 'dd-MM-yyyy'), to_char(e.f_inicio, 'HH24MI'), "
                    + "CASE WHEN e.path_imagen IS NULL THEN '' ELSE e.path_imagen END, e.m_estado_ruta_id "
                    + "FROM m_estado_ruta e INNER JOIN g_hoja_ruta r ON e.g_hoja_ruta_id = r.g_hoja_ruta_id "
                    + "INNER JOIN m_cliente cli ON cli.m_cliente_id = r.m_cliente_id "
                    + "INNER JOIN g_asignacion_hoja_ruta a ON a.g_hoja_ruta_id = r.g_hoja_ruta_id "
                    + "INNER JOIN m_cabeza c ON c.m_cabeza_id = a.m_cabeza_id "
                    + "INNER JOIN m_vehiculo v ON v.m_vehiculo_id = c.m_vehiculo_id "
                    + "WHERE cli.codigo = '2200' AND tipo_aux IS NOT NULL  AND tipo_aux != '' AND enviado_tiba = false "
                    + "and a.definitiva = true "
                    + "AND (SELECT (MAX(e2.tipo_aux) IS NULL OR MAX(e2.tipo_aux) < '41') "
                    + "FROM m_estado_ruta e2 WHERE e2.g_hoja_ruta_id = e.g_hoja_ruta_id AND e2.enviado_tiba = true);";
            try (Statement st = postgresqlConnection.createStatement(); ResultSet rs = st.executeQuery(selectSql)) {
                ResultSetMetaData rsmd = rs.getMetaData();
                int contador = 0;
                while (rs.next()) {
                    contador ++;
                    String numEvento = "Evento" + String.valueOf(contador);
                    String codigo = rs.getString(1);
                    String expedienteOL = rs.getString(2);
                    String numeroCont1 = rs.getString(3);
                    String numeroCont2 = rs.getString(4);
                    String tipoEvento = rs.getString(5);
                    String matricula = rs.getString(6);
                    String latitud = rs.getString(7).replace('.', ',');
                    String longitud = rs.getString(8).replace('.', ',');
                    String fecha = rs.getString(9);
                    String hora = rs.getString(10);
                    String imagenes = rs.getString(11);
                    String estadoId = rs.getString(12);

                    List<String> datos = new ArrayList<>();
                    datos.add(codigo);
                    datos.add(expedienteOL);
                    datos.add(numeroCont1);
                    datos.add(numeroCont2);
                    datos.add(tipoEvento);
                    datos.add(matricula);
                    datos.add(latitud);
                    datos.add(longitud);
                    ultimosEventos.put(numEvento, datos);

                    if (!tipoEvento.equals("00")) {
                        String[] imagenArray = imagenes.split(";");
                        List<String> imagenesList = new ArrayList<>();
                        imagenesList.addAll(Arrays.asList(imagenArray));
                        imagenesNameFile.put(estadoId, imagenesList);

                        BufferedWriter writer;
                        try {
                            writer = new BufferedWriter(new FileWriter(textFile, true));
                            writer.append(' ');
                            writer.append("\"" + codigo + "\";\"" + expedienteOL + "\";\"" + numeroCont1 + "\";\"" + numeroCont2 + "\";" + tipoEvento + ";\"" + matricula + "\";"
                                    + latitud + ";" + longitud + ";" + fecha + ";" + hora + ";\"" + imagenes + "\"\n");

                            writer.close();
                        } catch (IOException ex) {
                            Logger.getLogger(InterfazTIBA.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    } else {
                        // Añade el estado al mapa de imágenes aunque el estado sea de tipo 00, para poder actualizar enviado_tiba = true
                        imagenesNameFile.put(estadoId, new ArrayList<>());  
                    }
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(InterfazTIBA.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private static void anyadirEstadosPosicion() {
        ultimosEventos.forEach((t, u) -> {
            if (!u.get(4).equals("41")) {
                // Cambiar método de obtención de posición
                //List<String> posicion = getPosicionMovilData(u.get(5));
                List<String> posicion = getPosicionBBDD(u.get(5));
                String fecha = LocalDate.now().format(DateTimeFormatter.ofPattern("dd-MM-yyyy"));
                String hora = LocalTime.now().format(DateTimeFormatter.ofPattern("HHmm"));

                BufferedWriter writer;
                try {
                    writer = new BufferedWriter(new FileWriter(textFile, true));
                    writer.append(' ');
                    writer.append("\"" + u.get(0) + "\";\"" + u.get(1) + "\";\"" + u.get(2) + "\";\"" + u.get(3) + "\";1;\"" + u.get(5) + "\";"
                            + posicion.get(0) + ";" + posicion.get(1) + ";" + fecha + ";" + hora + ";\"\"\n");

                    writer.close();
                } catch (IOException ex) {
                    Logger.getLogger(InterfazTIBA.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        });
    }

    private static List<String> getPosicionMovilData(String matriculaCabeza) {
        List<String> posicion = new ArrayList<>();
        try {
            //Code to make a webservice HTTP request
            String responseString = "";
            String outputString = "";
            String wsURL = "https://ws.movildata.com/wsUsers.asmx?";
            URL url_asaas = new URL(wsURL);
            URLConnection connection = url_asaas.openConnection();
            HttpURLConnection httpConn = (HttpURLConnection) connection;
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            String xmlInput
                    = " <soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:wsus=\"http://ws.movildata.com/ws/wsUsers\">\n"
                    + " <soapenv:Header/>\n"
                    + " <soapenv:Body>\n"
                    + " <wsus:getLastLocationPlate>\n"
                    + " <!--Optional:-->\n"
                    + " <wsus:apikey>KO/TsiQRiqe0Pg+mGIeP+5RoG50sKv460pLZ9nKzVuJrUTfB6cUtqwNh5Tk7+Qxs</wsus:apikey>\n"
                    + " <!--Optional:-->\n"
                    + " <wsus:Plate>" + matriculaCabeza + "</wsus:Plate>\n"
                    + " </wsus:getLastLocationPlate>\n"
                    + " </soapenv:Body>\n"
                    + " </soapenv:Envelope>";
            byte[] buffer = new byte[xmlInput.length()];
            buffer = xmlInput.getBytes();
            bout.write(buffer);
            byte[] b = bout.toByteArray();
            String SOAPAction = "http://ws.movildata.com/ws/wsUsers/getLastLocationPlate";
            // Set the appropriate HTTP parameters.
            httpConn.setRequestProperty("Content-Length",
                    String.valueOf(b.length));
            httpConn.setRequestProperty("Content-Type", "text/xml; charset=utf-8");
            httpConn.setRequestProperty("SOAPAction", SOAPAction);
            httpConn.setRequestMethod("GET");
            httpConn.setDoOutput(true);
            httpConn.setDoInput(true);
            OutputStream out = httpConn.getOutputStream();
            //Write the content of the request to the outputstream of the HTTP Connection.
            out.write(b);
            out.close();
            //Ready with sending the request.

            //Read the response.
            InputStreamReader isr
                    = new InputStreamReader(httpConn.getInputStream(), StandardCharsets.UTF_8);
            BufferedReader in = new BufferedReader(isr);

            //Write the SOAP message response to a String.
            while ((responseString = in.readLine()) != null) {
                outputString = outputString + responseString;
            }

            SAXBuilder builder = new SAXBuilder();
            Reader in2 = new StringReader(outputString);
            Document doc;
            Element root;
            try {
                doc = builder.build(in2);
                root = doc.getRootElement();
                Element body = root.getChild("Body", Namespace.getNamespace("S", "http://schemas.xmlsoap.org/soap/envelope/"));
                Element getLastLocationPlateResponse = body.getChild("getLastLocationPlateResponse", Namespace.getNamespace("http://ws.movildata.com/ws/wsUsers"));
                Element getLastLocationPlateResult = getLastLocationPlateResponse.getChild("getLastLocationPlateResult", Namespace.getNamespace("http://ws.movildata.com/ws/wsUsers"));
                String line = getLastLocationPlateResult.getText();
                if (line.contains("<loc>")) {
                    String loc = line.substring(line.indexOf("<loc>") + "<loc>".length(), line.indexOf("</loc>"));
                    int colonPosition = loc.indexOf(",");
                    if (loc.length() > 3 && colonPosition != -1) {
                        String latitud = loc.substring(0, colonPosition).replace('.', ',');
                        String longitud = loc.substring(colonPosition + 1).replace('.', ',');
                        posicion.add(latitud);
                        posicion.add(longitud);
                    }
                }
            } catch (JDOMException ex) {
                LOGGER.log(Level.SEVERE, "JDOMException Matrícula: " + matriculaCabeza, ex.fillInStackTrace());
            }
        } catch (MalformedURLException ex) {
            LOGGER.log(Level.SEVERE, "MalformedURLException Matrícula: " + matriculaCabeza, ex.fillInStackTrace());
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, "IOException Matrícula: " + matriculaCabeza, ex.fillInStackTrace());
        }
        if (posicion.size() != 2) {
            posicion.clear();
            posicion.add("0");
            posicion.add("0");
        }
        return posicion;
    }

    public static void sendTextFileToTiba() {
        FTPClient ftpClient = new FTPClient();
        try {

            ftpClient.connect(ftpIp, Integer.valueOf(ftpPort));
            ftpClient.login(ftpUser, ftpPassword);
            ftpClient.enterLocalPassiveMode();
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
            String remoteFile = textFile.getName();
            InputStream inputStream = new FileInputStream(textFile);

            LOGGER.log(Level.SEVERE, "Start uploading text file");
            FileOutputStream outputStream2 = new FileOutputStream("./enviado/" + remoteFile);
            OutputStream outputStream = ftpClient.storeFileStream(remoteFile);
            byte[] bytesIn = new byte[4096];
            int read = 0;

            while ((read = inputStream.read(bytesIn)) != -1) {
                outputStream.write(bytesIn, 0, read);
                outputStream2.write(bytesIn, 0, read);
            }
            inputStream.close();
            outputStream.close();
            outputStream2.close();

            //boolean completed = true;
            boolean completed = ftpClient.completePendingCommand();
            if (completed) {
                LOGGER.log(Level.SEVERE, "The text file is uploaded successfully.");
            }

        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, null, ex);
            ex.printStackTrace();
        } finally {
             try {
                if (ftpClient.isConnected()) {
                    ftpClient.logout();
                    ftpClient.disconnect();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    private static void sendImagesToTiba() {
        LOGGER.log(Level.INFO, "Entra en función sendImagesToTiba");
        FTPClient ftpClient = new FTPClient();
        try {

            ftpClient.connect(ftpIp, Integer.valueOf(ftpPort));
            ftpClient.login(ftpUser, ftpPassword);
            ftpClient.enterLocalPassiveMode();
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
            for (String estadoId : imagenesNameFile.keySet()) {
                for (String imagenName : imagenesNameFile.get(estadoId)) {
                    if (!imagenName.isEmpty()) {
//                        File imageFile = new File(imagePath + imagenName);
//
//                        String remoteFile = imageFile.getName();
//                        InputStream inputStream = new FileInputStream(imageFile);
//
//                        FileOutputStream outputStream2 = new FileOutputStream("./enviado_img/" + remoteFile);
//                        OutputStream outputStream = ftpClient.storeFileStream(remoteFile);
//                        byte[] bytesIn = new byte[4096];
//                        int read = 0;
//
//                        while ((read = inputStream.read(bytesIn)) != -1) {
//                            outputStream.write(bytesIn, 0, read);
//                            outputStream2.write(bytesIn, 0, read);
//                        }
//                        inputStream.close();
//                        outputStream.close();
//                        outputStream2.close();
                        
                        String user = "EXOS";
                        String pass ="cbdriver@2021";
                        String sharedFolder="cbdriver/estados/";
//                        String sharedFolder="cbdriver/medlog/";
                        String ipAddress = "192.168.0.9";
                        String imgPath = "smb://" + ipAddress + "/" +sharedFolder+"/" + imagenName;
                        NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication("",user, pass);
                        
                        SmbFile smbFile = new SmbFile(imgPath,auth);  // Creamos SmbFile apuntando al directorio NAS donde están las fotos de las rutas
                        String smbRemoteFileName = smbFile.getName();   
                        SmbFileInputStream smbfis = new SmbFileInputStream(smbFile);
                        
                        // Opcional os2 (copia local)
                        FileOutputStream os2 = new FileOutputStream("./enviado_img/" + smbRemoteFileName);
                        LOGGER.log(Level.INFO, null, "Obtenido el output stream local: " + smbRemoteFileName);

                        // Obligatorio os (copia al FTP)
                        OutputStream os = ftpClient.storeFileStream(smbRemoteFileName);
                        LOGGER.log(Level.INFO, null, "Obtenido el output stream del dir FTP: " + imgPath);
                        
                        byte[] smbBytesIn = new byte[4096];
                        int smbRead = 0;

                        while ((smbRead = smbfis.read(smbBytesIn)) != -1) {
                            os.write(smbBytesIn, 0, smbRead);
                            os2.write(smbBytesIn, 0, smbRead);
                        }
                        LOGGER.log(Level.INFO, null, "Imágenes guardadas en los directorios FTP y local.");
                        smbfis.close();
                        os.close();
                        os2.close();
                    }
                }
            }

        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, null, ex);
            ex.printStackTrace();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, null, "Excepcion generica " + e);
            e.printStackTrace();
        } finally {
             try {
                if (ftpClient.isConnected()) {
                    ftpClient.logout();
                    ftpClient.disconnect();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    private static void updateEstadosEnviados() {
        String postgresqlUrl = "jdbc:postgresql://" + postgresqlIp + ":" + postgresqlPort + "/"
                + postgresqlDatabaseName + "?user=" + postgresqlUser + "&password=" + postgresqlPassword;

        String updateEstado = "UPDATE m_estado_ruta SET enviado_tiba = true WHERE m_estado_ruta_id = ?::uuid;";
        try (Connection con = DriverManager.getConnection(postgresqlUrl);
                PreparedStatement pst = con.prepareStatement(updateEstado)) {
            for (String estadoId : imagenesNameFile.keySet()) {
                pst.setString(1, estadoId);
                pst.addBatch();
                LOGGER.log(Level.INFO, null, "Enviado TIBA el estado: " + estadoId);
            }
            pst.executeBatch();
        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, null, ex);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, null, e.getMessage());
        }
    }

    private static List<String> getPosicionBBDD(String matricula) {
        List<String> posiciones = new ArrayList<>();
        
        try {
            Connection postgreCon = DriverManager.getConnection(POSTGRESQL_URL);

            // QUITAR LAS RUTAS CUYAS CABEZAS/PLATAFORMAS NO ESTÁN DADAS DE ALTA
            String selectSql = "SELECT pos.latitud, pos.longitud "
                    + " FROM m_ultima_posicion_tercero pos "
                    + " INNER JOIN m_tercero t ON t.m_tercero_id = pos.m_tercero_id "
                    + " INNER JOIN m_tercero_conductor tc ON tc.m_tercero_id = t.m_tercero_id "
                    + " INNER JOIN g_asignacion_hoja_ruta a ON a.m_tercero_cond_id = tc.m_tercero_conductor_id "
                    + " INNER JOIN g_hoja_ruta h ON h.g_hoja_ruta_id = a.g_hoja_ruta_id "
                    + " INNER JOIN m_cabeza c ON c.m_cabeza_id = a.m_cabeza_id "
                    + " INNER JOIN m_vehiculo v ON v.m_vehiculo_id = c.m_vehiculo_id"
                    + " WHERE v.matricula = ? AND a.definitiva AND h.f_planificada_inicio < cast((current_date + 1) as timestamp) "
                    + " ORDER BY h.f_planificada_llegada DESC LIMIT 1;";
            
            try (PreparedStatement ps = postgreCon.prepareStatement(selectSql)) {
                Logger.getLogger(InterfazTIBA.class.getName()).log(Level.INFO, "Obteniendo posición de la matrícula " + matricula +  ".");
                ps.setString(1, matricula);
                ResultSet rs = ps.executeQuery();
                while (rs.next()) {
                    posiciones.add(rs.getString(1));
                    posiciones.add(rs.getString(2));
                }
                Logger.getLogger(InterfazTIBA.class.getName()).log(Level.INFO, "Posición de la matrícula " + matricula + "obtenida: \n"
                        + "latitud: " + posiciones.get(0) + ", longitud: " + posiciones.get(1));
            }
            
        } catch (SQLException ex) {
                Logger.getLogger(InterfazTIBA.class.getName()).log(Level.SEVERE, null, ex);
        }
        if (posiciones.isEmpty()) {
            posiciones.add("");
            posiciones.add("");
            Logger.getLogger(InterfazTIBA.class.getName()).log(Level.WARNING, "Error al obtener la posición.");

        }
        return posiciones;
    }
}
