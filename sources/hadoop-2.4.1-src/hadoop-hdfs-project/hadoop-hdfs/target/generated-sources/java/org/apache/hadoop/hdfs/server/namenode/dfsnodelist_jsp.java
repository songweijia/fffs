package org.apache.hadoop.hdfs.server.namenode;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;
import org.apache.hadoop.util.ServletUtil;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;

public final class dfsnodelist_jsp extends org.apache.jasper.runtime.HttpJspBase
    implements org.apache.jasper.runtime.JspSourceDependent {


  //for java.io.Serializable
  private static final long serialVersionUID = 1L;

  private static java.util.List _jspx_dependants;

  public Object getDependants() {
    return _jspx_dependants;
  }

  public void _jspService(HttpServletRequest request, HttpServletResponse response)
        throws java.io.IOException, ServletException {

    JspFactory _jspxFactory = null;
    PageContext pageContext = null;
    HttpSession session = null;
    ServletContext application = null;
    ServletConfig config = null;
    JspWriter out = null;
    Object page = this;
    JspWriter _jspx_out = null;
    PageContext _jspx_page_context = null;


    try {
      _jspxFactory = JspFactory.getDefaultFactory();
      response.setContentType("text/html; charset=UTF-8");
      pageContext = _jspxFactory.getPageContext(this, request, response,
      			null, true, 8192, true);
      _jspx_page_context = pageContext;
      application = pageContext.getServletContext();
      config = pageContext.getServletConfig();
      session = pageContext.getSession();
      out = pageContext.getOut();
      _jspx_out = out;


/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file 
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


final NamenodeJspHelper.NodeListJsp nodelistjsp = new NamenodeJspHelper.NodeListJsp();
NameNode nn = NameNodeHttpServer.getNameNodeFromContext(application);
String namenodeRole = nn.getRole().toString();
FSNamesystem fsn = nn.getNamesystem();
HAServiceState nnHAState = nn.getServiceState();
boolean isActive = (nnHAState == HAServiceState.ACTIVE);
String namenodeLabel = NamenodeJspHelper.getNameNodeLabel(nn);

      out.write("<!DOCTYPE html>\n<html>\n\n<link rel=\"stylesheet\" type=\"text/css\" href=\"/static/hadoop.css\">\n<title>Hadoop ");
      out.print(namenodeRole);
      out.write("&nbsp;");
      out.print(namenodeLabel);
      out.write("</title>\n  \n<body>\n<h1>");
      out.print(namenodeRole);
      out.write(' ');
      out.write('\'');
      out.print(namenodeLabel);
      out.write("'</h1>\n");
      out.print( NamenodeJspHelper.getVersionTable(fsn) );
      out.write("<br />\n");
 if (isActive && fsn != null) { 
      out.write("<b><a href=\"/nn_browsedfscontent.jsp\">Browse the filesystem</a></b><br>\n");
 } 
      out.write("<b><a href=\"/logs/\">");
      out.print(namenodeRole);
      out.write(" Logs</a></b><br>\n<b><a href=/dfshealth.jsp> Go back to DFS home</a></b>\n<hr>\n");
 nodelistjsp.generateNodesList(application, out, request); 

out.println(ServletUtil.htmlFooter());

    } catch (Throwable t) {
      if (!(t instanceof SkipPageException)){
        out = _jspx_out;
        if (out != null && out.getBufferSize() != 0)
          out.clearBuffer();
        if (_jspx_page_context != null) _jspx_page_context.handlePageException(t);
      }
    } finally {
      if (_jspxFactory != null) _jspxFactory.releasePageContext(_jspx_page_context);
    }
  }
}
