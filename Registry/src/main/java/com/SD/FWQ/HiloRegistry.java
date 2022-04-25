import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.sql.*;

//HILOS DEL SERVIDOR CONCURRENTE (REGISTRY)

public class HiloRegistry extends Thread
{
    private Socket skCliente;

    public HiloRegistry(Socket p_cliente)
    {
        this.skCliente = p_cliente;
    }

    /*
     * Lee datos del socket. Supone que se le pasa un buffer con hueco
     *	suficiente para los datos. Devuelve el numero de bytes leidos o
     * 0 si se cierra fichero o -1 si hay error.
     */
    public String leeSocket (Socket p_sk, String p_Datos)
    {
        try
        {
            InputStream aux = p_sk.getInputStream();
            DataInputStream flujo = new DataInputStream( aux );
            p_Datos = new String();
            p_Datos = flujo.readUTF();
        }
        catch (Exception e)
        {
            System.out.println("Error: " + e);
        }
        return p_Datos;
    }

    /*
     * Escribe dato en el socket cliente. Devuelve numero de bytes escritos,
     * o -1 si hay error.
     */
    public void escribeSocket (Socket p_sk, String p_Datos)
    {
        try
        {
            OutputStream aux = p_sk.getOutputStream();
            DataOutputStream flujo= new DataOutputStream( aux );
            flujo.writeUTF(p_Datos);
        }
        catch (Exception e)
        {
            System.out.println("Error: " + e.toString());
        }
        return;
    }

    public boolean Operacion(String comando)
    {
        //System.out.println(comando);
        String[] datos = comando.split(",");
        Boolean ok = false;
        String devolver = "";

        if(datos.length >= 3)
        {
            escribeSocket(skCliente,comando);
            String op = datos[0];
            String id = datos[1],nombre,pass;
            
            if(datos.length == 4)
            {
				nombre = datos[2];
				pass = datos[3];
			}
			else
			{
				nombre = "";
				pass = datos[2];
			}

            switch(op)
            {
                case "1":	// Crear perfil
                    Insert(id,nombre,pass);
                    break;

                case "2":	// Editar perfil
                    Update(id,nombre,pass);
                    break;
                case "3":	// Comprobar Login
					if(Login(id,pass))
						this.escribeSocket(skCliente, id);
					else
						this.escribeSocket(skCliente,"incorrecto");
                    
                    break;
            }
            ok = true;
        }
        return ok;
    }

    public void run()
    {
        boolean ok = false;
        String Cadena = "", verificacion = "";

            try 
            {
                while(!ok)
                {
                    Cadena = this.leeSocket(skCliente, Cadena);
                    /*
                     * Se escribe en pantalla la informacion que se ha recibido del
                     * cliente
                     */
                     if(Cadena.length() != 0)
						System.out.println(Cadena);

						ok = this.Operacion(Cadena); // Función de Registro

                    //this.escribeSocket(skCliente, Cadena);

                    //System.exit(0); No se debe poner esta sentencia, porque en ese caso el primer cliente que cierra rompe el socket
                    //				  y desconecta a todos
                }
                System.out.println("DONE");
                skCliente.close();
            }
            catch (Exception e)
            {
                System.out.println("Error: " + e);
            }

    }

    private static Connection connect()
    {
       
		Connection connection = null;
		try
		{
		  Class.forName("org.sqlite.JDBC");
		  
		  // create a database connection
		  connection = DriverManager.getConnection("jdbc:sqlite:BDD.db");
		  Statement statement = connection.createStatement();
		  statement.setQueryTimeout(10);  // set timeout to 10 sec.
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return connection;
	}

    public boolean Update(String idclientes, String nombre , String pass)
    {
        boolean ok = false;
        String sql = "UPDATE clientes SET idclientes = ?, nombres= ?, password = ? WHERE idclientes = ? and password = ?;";

        try (Connection conn = this.connect();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // set the corresponding param
            pstmt.setString(1, idclientes.toString());
            pstmt.setString(2, nombre);
            pstmt.setString(3, pass);
            pstmt.setString(4, idclientes.toString());
            pstmt.setString(5, pass);

            // update
            pstmt.executeUpdate();
            ok = true;
        }
        catch (SQLException e)
        {
            System.out.println(e.getMessage());
            ok = false;
        }
        return ok;
    }
    public boolean Insert(String idclientes, String nombre,String pass)
    {
        boolean ok = false;
        String sql = "INSERT INTO clientes VALUES (?,?,?)";

        try (Connection conn = connect();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, idclientes);
            pstmt.setString(2, nombre);
            pstmt.setString(3,pass);
            pstmt.executeUpdate();
            ok = true;
        } catch (SQLException e) {
            ok = false;
            System.out.println(e.getMessage());
        }
        return ok;
    }
    public boolean Delete(String idclientes)
    {
        boolean ok = false;
        String sql = "DELETE FROM clientes WHERE idclientes = ?";

        try (Connection conn = connect();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // set the corresponding param
            pstmt.setString(1, idclientes.toString());
            // execute the delete statement
            pstmt.executeUpdate();
            ok = true;
        } catch (SQLException e) {
            ok = false;
            System.out.println(e.getMessage());
        }
        return ok;
    }

    public boolean Login(String idclientes,String pass)
    {
        boolean ok = false;
        String sql = "SELECT * FROM clientes WHERE idclientes = ?";

        try (Connection conn = connect();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // set the value
            pstmt.setString(1, idclientes);
            //
            ResultSet rs = pstmt.executeQuery();

            // loop through the result set
            while (rs.next()) 
            {
                if (rs.getString("password").equals(pass))
                    ok = true;
            }
        }
        catch (SQLException e) {
            ok = false;
            System.out.println(e.getMessage());
        }
        return ok;
    }
}
