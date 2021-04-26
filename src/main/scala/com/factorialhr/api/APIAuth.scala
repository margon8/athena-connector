package com.factorialhr.api

import com.google.gson.Gson
import scalaj.http.Http



class APIAuth {

  case class AuthorizationRequest(client_id: String, client_secret: String, code: String,
                                  grant_type: String = "authorization_code",
                                  redirect_uri: String = "http://www.google.com")

  case class AuthorizationResponse(access_token: String, token_type: String, expires_in: Int, refresh_token: String)

  def authAPIConnection(): String = {

    val appId = "T9nhsmjbm8wX_OYlbyijqEukZlyywwmmRMuPCoSzt6Q"
    val secret = "qLNW4uPSLjbeSjsE75P5Y-9NpS5Y2w1zIz4ZFM-xoyU"

    val authCode = "w7mAJqQZrn0u4eu48BcKdRkwvRF_DHFShu8IkfDfht8" //invalid because it no was assigned to this client

    val request = AuthorizationRequest(appId, secret, authCode)
    val requestBodyAsJson = new Gson().toJson(request)

    val authUrl = "https://api.factorialhr.com/oauth/token"

    val response = Http(authUrl)
      .postData(requestBodyAsJson)
      .header("Content-Type", "application/json")
      .header("Charset", "UTF-8")
      .asString.body

    val result = new Gson().fromJson(response, classOf[AuthorizationResponse])

    val bearer = result.access_token
    bearer
  }
}
